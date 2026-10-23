//! A stand-in HTTP API on this machine, for the tests of an adapter that reads an issuer's
//! (Ondo's in this crate): it answers each request by its path and counts them, until
//! dropped.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::poll::lock;

/// An HTTP server on this machine answering the routes it was given ([`Self::serve`]),
/// each with an answer written by [`Self::json`] or [`Self::answer`], and counting the
/// requests to each path ([`Self::hits`]). It stops when dropped.
pub struct MockApi {
    /// Where it listens: `http://127.0.0.1:<port>`, for an adapter's `api` setting.
    pub url: String,
    hits: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockApi {
    /// A 200 answering `body` as JSON.
    pub fn json(body: &str) -> String {
        Self::answer("200 OK", &[("content-type", "application/json")], body)
    }

    /// An answer with `status` (`302 Found`), `headers` and `body`. Every answer closes its
    /// connection, so a client never reuses one this server has hung up on.
    pub fn answer(status: &str, headers: &[(&str, &str)], body: &str) -> String {
        let mut answer = format!("HTTP/1.1 {status}\r\n");
        for (name, value) in headers {
            answer.push_str(&format!("{name}: {value}\r\n"));
        }
        answer.push_str(&format!(
            "content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ));
        answer
    }

    /// Answers a request for each path in `routes` with its answer, any other with a 404.
    pub fn serve(routes: Vec<(&'static str, String)>) -> MockApi {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (hits, stop) = (Arc::clone(&hits), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => reply(stream, &routes, &hits),
                        Err(_) => std::thread::sleep(Duration::from_millis(2)),
                    }
                }
            })
        };
        MockApi {
            url,
            hits,
            stop,
            thread: Some(thread),
        }
    }

    /// How many requests for `path` came in.
    pub fn hits(&self, path: &str) -> usize {
        lock(&self.hits).iter().filter(|hit| *hit == path).count()
    }
}

fn reply(mut stream: TcpStream, routes: &[(&str, String)], hits: &Mutex<Vec<String>>) {
    stream.set_nonblocking(false).unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") && matches!(stream.read(&mut byte), Ok(1)) {
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head);
    // `GET /path HTTP/1.1`
    let path = head.split(' ').nth(1).unwrap_or_default().to_owned();
    let not_found = MockApi::answer("404 Not Found", &[], "");
    let reply = routes
        .iter()
        .find(|(route, _)| *route == path)
        .map_or(&not_found, |(_, reply)| reply);
    lock(hits).push(path);
    let _ = stream.write_all(reply.as_bytes());
}

impl Drop for MockApi {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
