pub mod builder;
use log::{debug, error, info, warn}; // Import log macros
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use url::Url;
use rand::seq::SliceRandom; // Required for random supervisor selection

// Define potential errors
#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON serialization/deserialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("URL parsing failed: {0}")]
    Url(#[from] url::ParseError),
    #[error("No valid supervisor found for module: {0}")]
    NoSupervisor(String),
    #[error("Received unexpected status code: {0} from {1}")]
    UnexpectedStatus(reqwest::StatusCode, String),
    #[error("Missing Location header in 308 redirect")]
    MissingLocationHeader,
    #[error("Missing Supervisor-Locations header in 308 redirect")]
    MissingSupervisorLocationsHeader,
    #[error("Failed to parse Supervisor-Locations header: {0}")]
    InvalidSupervisorLocations(serde_json::Error),
    #[error("Maximum redirect attempts exceeded")]
    MaxRedirectsExceeded,
}

#[derive(Debug)]
pub struct Client {
    // Keep the original base URL (e.g., Conductor)
    base_url: Url,
    // Underlying HTTP client
    http_client: reqwest::Client,
    // Cache supervisor locations per module
    // Key: module_name, Value: list of supervisor host:port strings
    supervisor_cache: Arc<Mutex<HashMap<String, Vec<String>>>>,
    // Max redirects to follow
    max_redirects: u8,
}

impl Client {
    pub fn new(base_url: String) -> Result<Self, ClientError> {
        Ok(Self {
            base_url: Url::parse(&base_url)?,
            http_client: reqwest::Client::new(),
            supervisor_cache: Arc::new(Mutex::new(HashMap::new())),
            max_redirects: 5, // Sensible default
        })
    }

    // Core request sending logic with redirect handling (Refactored Style)
    async fn send_request<T: Serialize, R: DeserializeOwned>(
        &self,
        module: &str,
        path_suffix: &str, // e.g., "depot/*registerDepot/append" or "pstate/$$profiles/selectOne"
        body: &T,
    ) -> Result<R, ClientError> {
        let initial_url = self.build_url(module, path_suffix)?;
        let mut current_url = initial_url.clone();
        let mut attempts = 0;

        loop {
            // --- Guard: Max Redirects ---
            if attempts >= self.max_redirects { // Use >= for clarity (0..max_redirects attempts)
                error!("Maximum redirect attempts ({}) exceeded for request to module '{}', path '{}'", self.max_redirects, module, path_suffix);
                return Err(ClientError::MaxRedirectsExceeded);
            }
            attempts += 1;

            // --- Get Target URL ---
            let target_url = self.get_request_url(&current_url, module).await?;
            debug!("Attempt {} sending request to: {}", attempts, target_url);

            // --- Perform Request ---
            let response = self.http_client
                .post(target_url.clone())
                .header("Content-Type", "text/plain")
                .json(body)
                .send()
                .await
                .map_err(|e| {
                    // Add context to the HTTP error
                    error!("HTTP request to {} failed: {}", target_url, e);
                    ClientError::Http(e)
                })?;

            // --- Handle Status ---
            let status = response.status();

            // --- Success Case ---
            if status == reqwest::StatusCode::OK {
                debug!("Received OK status from {}", target_url);
                return response.json::<R>().await.map_err(|e| {
                    error!("Failed to deserialize OK response from {}: {}", target_url, e);
                    // Convert reqwest::Error into ClientError::Http explicitly if needed,
                    // or ensure ClientError::Http can be created from it.
                    // Assuming reqwest::Error maps correctly via #[from]
                    ClientError::Http(e)
                });
            }

            // --- Redirect Case ---
            if status == reqwest::StatusCode::PERMANENT_REDIRECT { // 308
                info!("Received 308 redirect from: {}", target_url);

                // Extract Location header
                let location_header_val = response.headers().get(reqwest::header::LOCATION)
                    .ok_or(ClientError::MissingLocationHeader)?;
                let location_str = location_header_val.to_str().map_err(|_| {
                    warn!("Location header contains non-ASCII characters from {}", target_url);
                    ClientError::MissingLocationHeader // Re-using error type, maybe add a specific one?
                })?;

                // Extract Supervisor-Locations header
                let supervisor_header_val = response.headers().get("Supervisor-Locations")
                    .ok_or_else(|| {
                        warn!("Missing Supervisor-Locations header in 308 from {}", target_url);
                        ClientError::MissingSupervisorLocationsHeader
                    })?;
                let supervisor_str = supervisor_header_val.to_str().map_err(|_| {
                    warn!("Supervisor-Locations header contains non-ASCII characters from {}", target_url);
                    ClientError::MissingSupervisorLocationsHeader // Re-using error type
                })?;

                // Parse Supervisors
                let supervisors: Vec<String> = serde_json::from_str(supervisor_str)
                    .map_err(|e| {
                        error!("Failed to parse Supervisor-Locations header ('{}') from {}: {}", supervisor_str, target_url, e);
                        ClientError::InvalidSupervisorLocations(e)
                    })?;

                 // Update cache
                debug!("Updating supervisor cache for module '{}' with: {:?}", module, &supervisors);
                // Note: lock guard is dropped immediately after use here.
                self.supervisor_cache.lock().unwrap() // Handle potential poisoning later
                    .insert(module.to_string(), supervisors);


                // Parse redirect URL and prepare for next attempt
                 match Url::parse(location_str) {
                     Ok(new_url) => {
                         current_url = new_url;
                         debug!("Following redirect to: {}", current_url);
                         continue; // Go to the next loop iteration
                     }
                     Err(e) => {
                         error!("Failed to parse Location header ('{}') from {}: {}", location_str, target_url, e);
                         return Err(ClientError::Url(e)); // Return error, cannot proceed
                     }
                 }
            }

            // --- Other Error Status ---
            // If we reach here, it's not OK or 308
            let error_body = response.text().await.unwrap_or_else(|_| "Could not read error body".to_string());
            error!(
                "Received unexpected status code {} from {}. Body: {}",
                status,
                target_url,
                error_body
            );
            // TODO: Implement retry logic for specific 5xx errors if desired
            // TODO: Potentially try another supervisor if available on 5xx
            return Err(ClientError::UnexpectedStatus(status, target_url.to_string()));
        }
    }

    // Helper to construct the initial URL
    fn build_url(&self, module: &str, path_suffix: &str) -> Result<Url, ClientError> {
         // Ensure base_url ends with '/', module doesn't start with '/', and path_suffix doesn't start with '/'
        let base = self.base_url.as_str().trim_end_matches('/');
        let module = module.trim_start_matches('/');
        let suffix = path_suffix.trim_start_matches('/');
        let full_path = format!("{}/rest/{}/{}", base, module, suffix);
        Url::parse(&full_path).map_err(ClientError::Url)
    }

    // Selects a URL to target, preferring cached supervisors
    async fn get_request_url(&self, base_request_url: &Url, module: &str) -> Result<Url, ClientError> {
        // --- Attempt to use cache ---
        let supervisor_list_opt = { // Lock scope
            let cache = self.supervisor_cache.lock().unwrap(); // Handle potential poisoning later
            cache.get(module).cloned() // Clone the Vec<String> if found
        };

        // Guard: No cache entry
        let Some(supervisor_list) = supervisor_list_opt else {
            debug!("No supervisor cache entry found for module '{}'. Using base/redirect URL: {}", module, base_request_url);
            return Ok(base_request_url.clone());
        };

        // Guard: Cache entry is empty list
        if supervisor_list.is_empty() {
            debug!("Supervisor list cache is empty for module '{}'. Using base/redirect URL: {}", module, base_request_url);
            return Ok(base_request_url.clone());
        }

        // --- Try selecting and parsing a supervisor ---
        let mut rng = rand::thread_rng();
        // Guard: Failed to choose random supervisor (unlikely if list is not empty)
        let Some(supervisor_host_port) = supervisor_list.choose(&mut rng) else {
            warn!("Failed to choose a supervisor from a non-empty list for module '{}'. Using base/redirect URL: {}", module, base_request_url);
             return Ok(base_request_url.clone());
        };

        // Guard: Supervisor string doesn't contain ':'
        let Some((host, port_str)) = supervisor_host_port.split_once(':') else {
            warn!("Supervisor host/port '{}' does not contain ':', cannot parse. Using base/redirect URL: {}", supervisor_host_port, base_request_url);
            return Ok(base_request_url.clone());
        };

        // Guard: Failed to parse port
        let Ok(port) = port_str.parse::<u16>() else {
            warn!("Failed to parse port '{}' from supervisor host/port '{}'. Using base/redirect URL: {}", port_str, supervisor_host_port, base_request_url);
            return Ok(base_request_url.clone());
        };

        // --- Try constructing the supervisor URL ---
        let mut supervisor_url = base_request_url.clone();
        // Guard: Failed to set host or port on the URL
        if supervisor_url.set_host(Some(host)).is_err() || supervisor_url.set_port(Some(port)).is_err() {
             warn!("Failed to set host/port ({}:{}) for supervisor URL based on {}. Using base/redirect URL.", host, port, base_request_url);
             return Ok(base_request_url.clone());
        }

        // --- Success: Use the constructed supervisor URL ---
        debug!("Using cached supervisor '{}' ({}) for module '{}'", supervisor_host_port, supervisor_url, module);
        Ok(supervisor_url)
    }

    // We will add builder methods here, e.g.,
    // pub fn pstate_query_builder(&self, module: &str, pstate: &str) -> PStateQueryBuilder { ... }
}       