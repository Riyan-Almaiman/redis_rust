use super::RedisCommand;

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "geosearch" => {
            if args.len() < 6 {
                return Err("GEOSEARCH requires key, longitude, latitude, and radius".into());
            }
            let longitude = args[2].parse::<f64>().map_err(|_| "Invalid longitude")?;
            let latitude = args[3].parse::<f64>().map_err(|_| "Invalid latitude")?;
            let radius = args[5].parse::<f64>().map_err(|_| "Invalid radius")?;
            Ok(RedisCommand::GeoSearch {
                key: args[0].to_string(),
                longitude,
                latitude,
                radius,
            })
        }
        "geodist" => {
            if args.len() < 3 {
                return Err("GEODIST requires key, member1, and member2".into());
            }
            Ok(RedisCommand::GeoDist {
                key: args[0].to_string(),
                member1: args[1].to_string(),
                member2: args[2].to_string(),
            })
        }
        "geopos" => {
            if args.len() < 2 {
                return Err("GEOPOS requires key and member".into());
            }

            Ok(RedisCommand::GeoPos {
                key: args[0].to_string(),
                members: args[1..].iter().map(|arg| arg.to_string()).collect(),
            })
        }
        "geoadd" => {
            if args.len() < 4 {
                return Err("GEOADD requires key, longitude, latitude, and member".into());
            }
            let longitude = args[1].parse::<f64>().map_err(|_| "Invalid longitude")?;
            let latitude = args[2].parse::<f64>().map_err(|_| "Invalid latitude")?;
            Ok(RedisCommand::GeoAdd {
                key: args[0].to_string(),
                longitude,
                latitude,
                member: args[3].to_string(),
            })
        }
        "zrem" => {
            if args.len() < 2 {
                return Err("invalid params".into());
            }
            Ok(RedisCommand::Zrem {
                key: args[0].to_string(),
                value: args[1].to_string(),
            })
        }
        "zscore" => {
            if args.len() < 2 {
                return Err("invalid params".into());
            }
            Ok(RedisCommand::Zscore {
                key: args[0].to_string(),
                value: args[1].to_string(),
            })
        }
        "zcard" => {
            if args.is_empty() {
                return Err("zcard requires a key".into());
            }
            Ok(RedisCommand::Zcard(args[0].to_string()))
        }
        "zrange" => {
            if args.len() < 3 {
                return Err("zrange requires key, start, end".into());
            }
            let start = args[1].parse::<isize>().map_err(|_| "Invalid start")?;
            let end = args[2].parse::<isize>().map_err(|_| "Invalid end")?;
            Ok(RedisCommand::Zrange {
                key: args[0].to_string(),
                start,
                end,
            })
        }
        "zrank" => {
            if args.len() < 2 {
                return Err("invalid params".into());
            }
            Ok(RedisCommand::Zrank {
                key: args[0].to_string(),
                values: args[1].to_string(),
            })
        }
        "zadd" => {
            if args.len() < 3 || args[1..].len() % 2 != 0 {
                return Err("invalid params".into());
            }

            let mut values = Vec::new();
            for chunk in args[1..].chunks(2) {
                let score = chunk[0].parse::<f64>().map_err(|_| "invalid value")?;
                values.push((score, chunk[1].to_string()));
            }

            Ok(RedisCommand::Zadd {
                key: args[0].to_string(),
                values,
            })
        }
        _ => Err(format!("Unknown command: {}", command)),
    }
}