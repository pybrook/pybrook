use redis_module::key::{HMGetResult};
use redis_module::logging::log_warning;
use redis_module::{raw, redis_module, Context, RedisModuleStreamID, NotifyEvent, RedisModule_StreamAdd, RedisResult, RedisString, RedisValue, Status, REDISMODULE_STREAM_ADD_AUTOID, RedisError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap};
use std::fmt::Display;
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::str;
use std::sync::{LazyLock, RwLock};
use redis_module_macros::command;


const MSG_ID_FIELD: &str = "@pb@msg_id";
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegularDependencyField {
    src: String,
    dst: String,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoricalDependencyField {
    src: String,
    dst: String,
    history_len: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum DependencyField {
    Regular(RegularDependencyField),
    Historical(HistoricalDependencyField),
}

impl HistoricalDependencyField {
    fn hmap_key(&self, n: u8) -> String {
        format!("{}:{}", self.dst, n)
    }
}


impl DependencyField {

    fn dst_keys(&self) -> Vec<String> {
        match self {
            DependencyField::Regular(regular) => { vec![regular.dst.clone()] }
            DependencyField::Historical(historical) => {
                (0..historical.history_len).collect::<Vec<u8>>().into_iter().map(|i| historical.hmap_key(i)).collect()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DependencyBase {
    stream_key: String,fields: Vec<DependencyField>,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(from = "DependencyBase")]
struct Dependency {
    stream_key: String, // could be some kind of wildcard in the future,
    // to support partitioning
    // tagging would still work the same way
    fields: Vec<DependencyField>,
    has_historical_fields: bool,
    has_regular_fields: bool
}

impl From<DependencyBase> for Dependency {

    fn from(value: DependencyBase) -> Self {
        Self {
            stream_key: value.stream_key,
            has_historical_fields: value.fields.iter().any(|f| {
                matches!(f, DependencyField::Historical(_))
            }),
            has_regular_fields: value.fields.iter().any(|f| {
                matches!(f, DependencyField::Regular(_))
            }),
            fields: value.fields,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DependencyResolver {
    inputs: Vec<Dependency>,
    output_stream_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputTagger {
    stream_key: String,
    obj_id_field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrookConfig {
    dependency_resolvers: HashMap<String, DependencyResolver>,
    input_taggers: HashMap<String, InputTagger>,
}

#[derive(Debug)]
enum DependencyResolverError {
    MissingIdField,
    #[allow(dead_code)]
    MalformedIdField(String),
    HMGETError,
    KeyDropError,
}

impl DependencyResolver {
    fn dep_maps_key(&self, ctx: &Context, message_id: impl Display) -> RedisString{
        redis_string(
            ctx,
            format!("__{}:{}@pb@dmap__", self.output_stream_key, message_id),
        )
    }
    fn process_message(
        &self,
        ctx: &Context,
        dependency: &Dependency,
        message: &Map<String, Value>,
    ) -> Result<(), DependencyResolverError> {

        let message_id = message
            .get(MSG_ID_FIELD)
            .ok_or(DependencyResolverError::MissingIdField)?;
        let message_id_string = match message_id {
            Value::String(s) => { Some(s) }
            _ => None
        }.ok_or(DependencyResolverError::MalformedIdField("not a string".into()))?;
        let total_dependencies: i64 = self.inputs.iter().filter(|d| d.has_regular_fields).count() as i64;
        let mut message_cloned = message.clone();

        let mut dependency_map = serde_json::Map::from_iter(
            dependency
                .fields
                .clone()
                .into_iter()
                .filter_map(|f| {
                    match f {
                        DependencyField::Regular(regular) => {
                            Some((
                                regular.dst,
                                message_cloned.remove(&regular.src).unwrap_or(Value::Null),
                            ))
                        }
                        DependencyField::Historical { .. } => { None }
                    }
                })
        );

        let deps_map_key = self.dep_maps_key(ctx, message_id_string);
        let dcount_key = redis_string(
            ctx,
            format!("__{}:{}@pb@dcount__", self.output_stream_key, message_id_string),
        );
        if dependency.has_regular_fields && incr(ctx, &dcount_key).eq(&total_dependencies) {
            let key = ctx.open_key(&deps_map_key);
            let fields: Vec<RedisString> = self
                .inputs
                .iter()
                .flat_map(|d| d.fields.iter().flat_map(|f| f.dst_keys()).map(|key| redis_string(ctx, key)))
                .collect();

            // note: hash_get_multi sends fields in 12-element chunks, because of varargs API limitations:
            // https://github.com/redis/redis/issues/7860
            let values: Option<HMGetResult<_, String>> = key
                .hash_get_multi(&fields)
                .map_err(|_| DependencyResolverError::HMGETError)?;
            let mut dependency_redis_hmap: Map<String, Value> = match values {
                None => Map::new(),
                Some(values) => Map::from_iter(
                    values
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                k.to_string(),
                                serde_json::from_str(&v).unwrap_or(Value::Null),
                            )
                        })
                        .collect::<HashMap<String, Value>>(),
                ),
            };
            for historical_field in self.inputs.iter().flat_map(|f| &f.fields).filter_map(|f| match f {
                DependencyField::Historical(h) => {Some(h)}
                _ => {None}
            }) {
                let mut historical_values: Vec<Value> = vec![];
                for i in 0..historical_field.history_len {
                    let value = dependency_redis_hmap.remove(&historical_field.hmap_key(i)).unwrap_or(Value::Null);
                    historical_values.push(value);
                }
                dependency_redis_hmap.insert(historical_field.dst.clone(), Value::Array(historical_values));
            }
            dependency_redis_hmap.append(&mut dependency_map);
            dependency_redis_hmap.insert(MSG_ID_FIELD.to_string(), message_id.clone());
            stream_add(ctx, self.output_stream_key.as_bytes(), &Value::from(dependency_redis_hmap));
            let key = ctx.open_key_writable(&deps_map_key);
            key.delete()
                .map_err(|_| DependencyResolverError::KeyDropError)?;
            ctx.open_key_writable(&dcount_key)
                .delete()
                .map_err(|_| DependencyResolverError::KeyDropError)?;
        } else {
            let key = ctx.open_key_writable(&deps_map_key);
            for (field, value) in dependency_map {
                key.hash_set(
                    &field,
                    redis_string(ctx, serde_json::to_string(&value).unwrap_or("null".into())),
                );
            }
        }
        if dependency.has_historical_fields {
            let mut splitter = message_id_string.rsplitn(2,":");
            let message_id: u64 = splitter.next().ok_or(DependencyResolverError::MalformedIdField(message_id_string.clone()))?.parse::<u64>().map_err(|_| DependencyResolverError::MalformedIdField(message_id.to_string()))?;
            let object_id = splitter.next().ok_or(DependencyResolverError::MalformedIdField(message_id_string.clone()))?;
            for field in dependency.fields.iter().filter_map(|f| match f {
                DependencyField::Regular(_) => {None}
                DependencyField::Historical(h) => {Some(h)}
            }) {
                let mut future_obj_message_id = message_id;
                for id_in_deps in (0..field.history_len).rev() {
                    future_obj_message_id += 1;
                    let key = ctx.open_key_writable(&self.dep_maps_key(ctx, format!("{object_id}:{future_obj_message_id}")));
                    let value = message.get(&field.src).unwrap_or(&Value::Null);
                    key.hash_set(&field.hmap_key(id_in_deps), redis_string(ctx, serde_json::to_string(&value).unwrap_or("null".into())));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
enum TaggerError {
    InvalidIdField,
    CouldNotParseId,
}

fn incr(ctx: &Context, key_redis_str: &RedisString) -> i64 {
    let incr_key = ctx.open_key(key_redis_str);
    let value: i64 = String::from_utf8_lossy(incr_key.read().unwrap().unwrap_or(&[]))
        .parse()
        .unwrap_or(0);
    let incr_key_writable = ctx.open_key_writable(key_redis_str);
    incr_key_writable.write(&(value + 1).to_string()).unwrap();
    value + 1
}

impl InputTagger {
    fn tag_message(
        &self,
        ctx: &Context,
        message: &mut Map<String, Value>,
    ) -> Result<(), TaggerError> {
        let object_id: String = match message
            .get(&self.obj_id_field)
            .ok_or(TaggerError::InvalidIdField)?
        {
            Value::Bool(p) => Some(p.to_string()),
            Value::Number(n) => Some(n.to_string()),
            Value::String(s) => Some(s.to_string()),
            _ => None,
        }
            .ok_or(TaggerError::CouldNotParseId)?;
        message.insert(
            MSG_ID_FIELD.into(),
            format!("{}:{}", object_id, incr(
                ctx,
                &redis_string(
                    ctx,
                    format!("__{}:{}:{}__", self.stream_key, object_id, MSG_ID_FIELD),
                ),
            )).into(),
        );
        Ok(())
    }
}

static PB_CONFIG: LazyLock<RwLock<BrookConfig>> = LazyLock::new(|| {
    let state = BrookConfig {
        dependency_resolvers: HashMap::new(),
        input_taggers: HashMap::new(),
    };
    RwLock::new(state)
});

fn gen_resolver_map() -> HashMap<String, Vec<(DependencyResolver, Dependency)>> {
    let state = PB_CONFIG.read().unwrap();
    let mut map = HashMap::new();
    for resolver in state.dependency_resolvers.values() {
        for (stream_key, dependency) in resolver.inputs.iter().map(|d| (d.stream_key.clone(), d)) {
            map.entry(stream_key)
                .or_insert(Vec::new())
                .push((resolver.clone(), dependency.clone()));
        }
    }
    map
}

#[allow(clippy::type_complexity)]
static RESOLVER_MAP: LazyLock<
    RwLock<HashMap<String, Vec<(DependencyResolver, Dependency)>>>,
> = LazyLock::new(|| {
    let map = gen_resolver_map();
    RwLock::new(map)
});

#[inline]
fn redis_string<T: Into<Vec<u8>>>(ctx: &Context, value: T) -> RedisString {
    RedisString::create(NonNull::new(ctx.ctx), value)
}

fn stream_add(ctx: &Context, key_name: &[u8], message: &Value) -> Status {
    let mut id = RedisModuleStreamID { ms: 0, seq: 0 };
    let stream_message_id = &mut id as *mut RedisModuleStreamID;

    let mut message_vector: Vec<RedisString> = vec![];
    for (key, value) in message.as_object().unwrap() {
        message_vector.push(redis_string(ctx, key.as_str()));
        message_vector.push(redis_string(
            ctx,
            serde_json::to_string(value).unwrap().as_bytes(),
        ));
    }
    let mut args = message_vector.iter().map(|v| v.inner).collect::<Vec<_>>();

    let key = raw::open_key(
        ctx.ctx,
        redis_string(ctx, key_name).inner,
        raw::KeyMode::WRITE,
    );
    let status: Status = unsafe {
        RedisModule_StreamAdd.unwrap()(
            key,
            REDISMODULE_STREAM_ADD_AUTOID as c_int,
            stream_message_id,
            args.as_mut_ptr(),
            (message_vector.len() as i64) / 2,
        )
    }.into();
    raw::close_key(key);
    return status;
}

fn on_stream(ctx: &Context, _event_type: NotifyEvent, event: &str, key: &'static [u8]) {
    if event != "xadd" {
        return;
    }
    let key_string = redis_string(ctx, key);
    let stream = ctx.open_key(&key_string);

    let state = PB_CONFIG.read().unwrap();
    let resolver_map = RESOLVER_MAP.read().unwrap();
    let stream_key_str = str::from_utf8(key).expect("Only UTF-8 keys are supported!");
    let tagger = state.input_taggers.get(stream_key_str);

    let mut last_record_id = RedisModuleStreamID { ms: 0, seq: 0 };
    for record in stream
        .get_stream_range_iterator(None, None, true, false)
        .unwrap()
    {
        let mut message = serde_json::Map::new();
        for field in record.fields {
            let key = field.0.to_string();
            let value_json = field.1.to_string();
            let value = serde_json::from_str(value_json.as_str()).unwrap_or(Value::Null);
            message.insert(key, value);
        }
        last_record_id = record.id;
        if let Some(input_tagger) = tagger {
            if let Err(e) = input_tagger.tag_message(ctx, &mut message) {
                log_warning(format!(
                    "WARNING: Tagger error {e:?} for stream {stream_key_str}"
                ));
                continue;
            }
        }
        if let Some(dependency_resolvers) = resolver_map.get(stream_key_str) {
            for (resolver, dependency) in dependency_resolvers {
                resolver
                    .process_message(ctx, dependency, &message)
                    .unwrap();
            }
        }
    }
    let stream = ctx.open_key_writable(&key_string);
    stream
        .trim_stream_by_id(
            RedisModuleStreamID {
                ms: last_record_id.ms,
                seq: u64::MAX,
            },
            false,
        )
        .unwrap();
}

#[command(
    {
    name: "pb.setconfig",
    summary: "sets the PyBrook Config",
    flags: [Write],
    arity: 2,
    key_spec: [
    {
    flags: [ReadWrite],
    begin_search: Index({ index: 1 }),
    find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
    }
    ]
    }
)]
fn set_config(_ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut config = PB_CONFIG.write()?;
    let string = args.get(1).ok_or(RedisError::WrongType)?.to_string();
    let cfg = serde_json::from_str::<BrookConfig>(&string)?;
    *config = cfg.clone();
    drop(config);
    let mut resolver_map = RESOLVER_MAP.write()?;
    println!("Resolver map write!");
    *resolver_map = gen_resolver_map();
    println!("Stored config: {}", &string);
    Ok(RedisValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use Value;

    #[test]
    fn it_works() {
        let val: Value = serde_json::from_str(String::from_str("\"x\"").unwrap().as_str()).unwrap();
    }
}

fn init(_ctx: &Context, _args: &[RedisString]) -> Status {
    Status::Ok
}

fn deinit(_ctx: &Context) -> Status {
    Status::Ok
}

#[cfg(not(test))]
redis_module! {
    name: "pybrook",
    version: 1,
    allocator: (redis_module::alloc::RedisAlloc, redis_module::alloc::RedisAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
    ],
    event_handlers: [
        [@STREAM: on_stream],
    ]
}
