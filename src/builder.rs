use crate::{Client, ClientError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

// --- Helper functions for Rama Special Types ---

/// Creates a JSON string value representing a Rama Long.
pub fn rama_long(val: i64) -> Value {
    Value::String(format!("#__L{}", val))
}

/// Creates a JSON string value representing a Rama Byte.
pub fn rama_byte(val: i8) -> Value {
    Value::String(format!("#__B{}", val))
}

/// Creates a JSON string value representing a Rama Short.
pub fn rama_short(val: i16) -> Value {
    Value::String(format!("#__S{}", val))
}

/// Creates a JSON string value representing a Rama Float.
pub fn rama_float(val: f32) -> Value {
    Value::String(format!("#__F{}", val))
}

/// Creates a JSON string value representing a Rama Char.
pub fn rama_char(val: char) -> Value {
    Value::String(format!("#__C{}", val))
}

/// Creates a JSON string value representing a Rama Clojure Keyword.
pub fn rama_keyword(val: &str) -> Value {
    Value::String(format!("#__K{}", val))
}

/// Creates a JSON string value representing a Rama Function reference.
pub fn rama_function(name: &str) -> Value {
    Value::String(format!("#__f{}", name))
}

/// Helper for referencing built-in Ops functions, e.g., Ops.IS_EVEN
pub fn rama_ops_function(name: &str) -> Value {
    rama_function(&format!("Ops.{}", name))
}


// --- PState Query Builder ---

/// Builds a PState query path.
///
/// Use the methods to add navigators to the path, then call `select` or `select_one`.
#[derive(Debug)]
pub struct PStateQueryBuilder<'a> {
    // Need a mutable reference or owned client? Let's try shared ref first.
    client: &'a Client,
    module: String,
    pstate: String,
    path: Vec<Value>,
}

impl<'a> PStateQueryBuilder<'a> {
    pub(crate) fn new(client: &'a Client, module: &str, pstate: &str) -> Self {
        Self {
            client,
            module: module.to_string(),
            pstate: pstate.to_string(),
            path: Vec::new(),
        }
    }

    // --- Implicit Navigators ---

    /// Adds an implicit navigator (e.g., String, number, boolean, null, special type).
    /// Often equivalent to `key` for strings/keywords or `filterPred` for functions.
    pub fn nav(mut self, value: impl Into<Value>) -> Self {
        self.path.push(value.into());
        self
    }

    /// Adds a key navigator (implicitly wraps the string key).
    pub fn key(self, key: impl Into<String>) -> Self {
        self.nav(key.into())
    }

    /// Adds a filterPred navigator using a Rama function reference (e.g., "#__fOps.IS_EVEN").
    pub fn filter_pred_fn(self, function_name: &str) -> Self {
         self.nav(rama_function(function_name))
    }

    // --- Explicit Navigators (Examples) ---
    // These construct a JSON array: `["opName", arg1, arg2, ...]`

    fn add_explicit_nav(mut self, op: &str, args: Vec<Value>) -> Self {
        let mut nav_array = vec![Value::String(op.to_string())];
        nav_array.extend(args);
        self.path.push(Value::Array(nav_array));
        self
    }

    /// Adds the "all" navigator: `["all"]`.
    pub fn all(self) -> Self {
        self.add_explicit_nav("all", vec![])
    }

    /// Adds the "must" navigator: `["must", key1, key2, ...]`.
    pub fn must(self, keys: impl IntoIterator<Item = impl Into<Value>>) -> Self {
        self.add_explicit_nav("must", keys.into_iter().map(Into::into).collect())
    }

    /// Adds the "mapVals" navigator: `["mapVals"]`.
    pub fn map_vals(self) -> Self {
         self.add_explicit_nav("mapVals", vec![])
    }

    /// Adds a "filterSelected" navigator: `["filterSelected", path...]`.
    /// The path itself is represented as a Vec<Value>.
    pub fn filter_selected(mut self, path_to_filter: Vec<Value>) -> Self {
        let mut nav_array = vec![Value::String("filterSelected".to_string())];
        // The path_to_filter is treated as *one* argument which is a path
        nav_array.extend(path_to_filter); // Extend directly flattens path, is this right?
        // No, the doc says: ["filterSelected", "a", ["all"], #__fOps.IS_EVEN"]
        // Java: Path.filterSelected(Path.key("a").all().filterPred(Ops.IS_EVEN))
        // It seems filterSelected takes the *components* of the sub-path as its arguments.
        self.path.push(Value::Array(nav_array));
        self
    }

     /// Adds a "subselect" navigator: `["subselect", path...]`.
     /// Similar interpretation to filterSelected regarding path arguments.
    pub fn subselect(mut self, sub_path: Vec<Value>) -> Self {
        let mut nav_array = vec![Value::String("subselect".to_string())];
        nav_array.extend(sub_path);
        self.path.push(Value::Array(nav_array));
        self
    }

    // Add more explicit navigator methods here based on the documentation...
    // e.g., multiPath, view, termVal, sortedMapRange, etc.

    // --- Execution Methods ---

    /// Executes the query using the constructed path via the `select` endpoint.
    /// Expects a list of results.
    pub async fn select<R: DeserializeOwned>(self) -> Result<Vec<R>, ClientError> {
        let path_suffix = format!("pstate/{}/select", self.pstate);
        // The body for PState queries is the JSON array representing the path
        self.client
            .send_request(&self.module, &path_suffix, &self.path)
            .await
    }

    /// Executes the query using the constructed path via the `selectOne` endpoint.
    /// Expects a single result. Errors if 0 or >1 results are found by the server.
    pub async fn select_one<R: DeserializeOwned>(self) -> Result<R, ClientError> {
        let path_suffix = format!("pstate/{}/selectOne", self.pstate);
        // The body is the same path array
        self.client
            .send_request(&self.module, &path_suffix, &self.path)
            .await
    }
}


// --- Depot Append Builder ---

/// Represents the acknowledgment levels for depot appends.
#[derive(Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "camelCase")] // Match Rama's expected case "appendAck"
pub enum AckLevel {
    /// Waits for the record to be fully processed by streaming topologies. (Default)
    Ack,
    /// Waits only for the record to be appended to the depot partition.
    AppendAck,
    /// Does not wait for any acknowledgment.
    None,
}

// Private struct for the request body
#[derive(Serialize)]
struct DepotAppendBody<T: Serialize> {
    data: T,
    #[serde(rename = "ackLevel", skip_serializing_if = "Option::is_none")]
    ack_level: Option<AckLevel>,
}

/// Builds a Depot append request.
#[derive(Debug)]
pub struct DepotAppendBuilder<'a, T: Serialize> {
    client: &'a Client,
    module: String,
    depot: String,
    data: T, // Data is required
    ack_level: Option<AckLevel>, // Defaults to server default ("ack") if None
}

impl<'a, T: Serialize> DepotAppendBuilder<'a, T> {
     pub(crate) fn new(client: &'a Client, module: &str, depot: &str, data: T) -> Self {
        Self {
            client,
            module: module.to_string(),
            depot: depot.to_string(),
            data,
            ack_level: None,
        }
    }

    /// Sets the acknowledgment level for the append operation.
    /// If not called, the server default ("ack") is used.
    pub fn ack_level(mut self, level: AckLevel) -> Self {
        self.ack_level = Some(level);
        self
    }

    /// Executes the depot append request.
    ///
    /// The type `R` depends on the `ackLevel`:
    /// - `AckLevel::Ack`: `HashMap<String, Value>` (topology name -> ack return value)
    /// - `AckLevel::AppendAck` or `AckLevel::None`: `serde_json::Value::Object` (empty map `{}`)
    pub async fn append<R: DeserializeOwned>(self) -> Result<R, ClientError> {
        let body = DepotAppendBody {
            data: self.data,
            ack_level: self.ack_level,
        };
        let path_suffix = format!("depot/{}/append", self.depot);
        self.client
            .send_request(&self.module, &path_suffix, &body)
            .await
    }
}
