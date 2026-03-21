use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use crate::db::Client;
use crate::resp::Resp;

pub  fn send_cmd(response_tx: UnboundedSender<Vec<u8>>, resp: Resp) {

        let mut buffer = Vec::new();

        resp.write_format(&mut buffer);
 let _ =     response_tx.send(buffer);

}
