use super::{RedisCommand, StreamEntryIdCommandType, StreamRead};

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "xadd" => parse_xadd(args),
        "xrange" => parse_xrange_command(args),
        "xread" => parse_xread(args),
        _ => Err(format!("Unknown command: {}", command)),
    }
}

fn parse_xadd(args: &[&str]) -> Result<RedisCommand, String> {
    if args.len() < 3 || (args.len() - 2) % 2 != 0 {
        return Err("XADD requires key, ID, and field-value pairs".into());
    }

    let key = args[0].as_bytes().to_vec();
    let id_str = args[1];

    let mut fields = Vec::new();
    let mut index = 2;
    while index + 1 < args.len() {
        fields.push((
            args[index].as_bytes().to_vec(),
            args[index + 1].as_bytes().to_vec(),
        ));
        index += 2;
    }

    if id_str == "*" {
        return Ok(RedisCommand::XAdd {
            key,
            fields,
            id: StreamEntryIdCommandType::GenerateTimeAndSequence,
        });
    }

    let (time_str, seq_str) = id_str.split_once('-').ok_or("Invalid ID format")?;
    let time = time_str
        .parse::<u64>()
        .map_err(|_| "Invalid ID timestamp")?;

    match seq_str {
        "*" => Ok(RedisCommand::XAdd {
            key,
            fields,
            id: StreamEntryIdCommandType::GenerateOnlySequence { time },
        }),
        _ => {
            let sequence = seq_str.parse::<u64>().map_err(|_| "Invalid ID sequence")?;
            Ok(RedisCommand::XAdd {
                key,
                fields,
                id: StreamEntryIdCommandType::Explicit { time, sequence },
            })
        }
    }
}

fn parse_xrange_command(args: &[&str]) -> Result<RedisCommand, String> {
    if args.len() < 3 {
        return Err("XRANGE requires key start end".into());
    }

    parse_xrange(args[0].as_bytes().to_vec(), args[1], args[2])
}

fn parse_xrange(key: Vec<u8>, start_str: &str, end_str: &str) -> Result<RedisCommand, String> {
    fn parse_id(id: &str, is_start: bool) -> Result<(u64, u64), String> {
        if id == "-" {
            return Ok((0, 0));
        }
        if id == "+" {
            return Ok((u64::MAX, u64::MAX));
        }

        if let Some((time, sequence)) = id.split_once('-') {
            Ok((
                time.parse::<u64>().map_err(|_| "Invalid time")?,
                sequence.parse::<u64>().map_err(|_| "Invalid sequence")?,
            ))
        } else {
            let time = id.parse::<u64>().map_err(|_| "Invalid ID")?;
            if is_start {
                Ok((time, 0))
            } else {
                Ok((time, u64::MAX))
            }
        }
    }

    let (start_time, start_sequence) = parse_id(start_str, true)?;
    let (end_time, end_sequence) = parse_id(end_str, false)?;

    Ok(RedisCommand::XRange {
        key,
        start_time,
        end_time,
        start_sequence,
        end_sequence,
    })
}

fn parse_xread(args: &[&str]) -> Result<RedisCommand, String> {
    let streams_pos = args
        .iter()
        .position(|arg| arg.eq_ignore_ascii_case("streams"))
        .ok_or("Missing STREAMS keyword")?;

    let block_pos = args.iter().position(|arg| arg.eq_ignore_ascii_case("block"));
    let timeout = if let Some(pos) = block_pos {
        let timeout_arg = args.get(pos + 1).ok_or("Invalid timeout")?;
        Some(timeout_arg.parse::<f64>().map_err(|_| "Invalid timeout")?)
    } else {
        None
    };

    let stream_args = &args[streams_pos + 1..];
    if stream_args.len() % 2 != 0 {
        return Err("Invalid stream/id pairs".into());
    }

    let half = stream_args.len() / 2;
    let mut streams = Vec::new();
    for index in 0..half {
        streams.push(StreamRead {
            key: stream_args[index].as_bytes().to_vec(),
            id: stream_args[index + half].to_string(),
        });
    }

    Ok(RedisCommand::XRead { streams, timeout })
}