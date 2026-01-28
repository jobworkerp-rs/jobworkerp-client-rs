use crate::display::DisplayFormat;
use crate::jobworkerp::data::WorkerInstance;
use crate::jobworkerp::service::InstanceChannelInfo;
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

fn format_timestamp(millis: i64, format: &DisplayFormat) -> JsonValue {
    match format {
        DisplayFormat::Json => serde_json::json!(millis),
        _ => {
            let datetime = DateTime::<Utc>::from_timestamp_millis(millis);
            match datetime {
                Some(dt) => serde_json::json!(dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
                None => serde_json::json!(millis),
            }
        }
    }
}

pub fn worker_instance_to_json(instance: &WorkerInstance, format: &DisplayFormat) -> JsonValue {
    let id = instance.id.as_ref().map(|id| id.value);
    let (ip_address, hostname, channels, registered_at, last_heartbeat) = if let Some(data) =
        &instance.data
    {
        let channels: Vec<JsonValue> = data
                .channels
                .iter()
                .map(|ch| {
                    serde_json::json!({
                        "name": if ch.name.is_empty() { "[default]".to_string() } else { ch.name.clone() },
                        "concurrency": ch.concurrency,
                    })
                })
                .collect();
        (
            Some(data.ip_address.clone()),
            data.hostname.clone(),
            channels,
            Some(format_timestamp(data.registered_at, format)),
            Some(format_timestamp(data.last_heartbeat, format)),
        )
    } else {
        (None, None, vec![], None, None)
    };

    serde_json::json!({
        "id": id,
        "ip_address": ip_address,
        "hostname": hostname,
        "channels": channels,
        "registered_at": registered_at,
        "last_heartbeat": last_heartbeat,
    })
}

pub fn instance_channel_info_to_json(
    info: &InstanceChannelInfo,
    format: &DisplayFormat,
) -> JsonValue {
    let _ = format;
    serde_json::json!({
        "name": info.name,
        "total_concurrency": info.total_concurrency,
        "active_instances": info.active_instances,
        "worker_count": info.worker_count,
    })
}
