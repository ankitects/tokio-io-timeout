//! Tokio wrappers which apply timeouts to IO operations.
//!
//! These timeouts are analagous to the read and write timeouts on traditional
//! blocking sockets. A timeout countdown is initiated when a read/write
//! operation returns `WouldBlock`. If a read/write does not return successfully
//! the before the countdown expires, `TimedOut` is returned.
#![doc(html_root_url = "https://docs.rs/tokio-io-timeout/0.4")]
#![warn(missing_docs)]

use bytes::{Buf, BufMut};
use std::future::Future;
use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{delay_until, Delay, Instant};

#[derive(Debug)]
struct TimeoutState {
    /// If set, the timeout to apply to I/O operations
    timeout: Option<Duration>,
    /// The currently active timer future.
    timer: Delay,
    /// The last time a read or write was performed.
    last_io: Option<Instant>,
}

impl TimeoutState {
    #[inline]
    fn new() -> TimeoutState {
        TimeoutState {
            timeout: None,
            timer: delay_until(Instant::now()),
            last_io: None,
        }
    }

    #[inline]
    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    #[inline]
    fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
        self.last_io = None;
    }

    #[inline]
    fn reset_elapsed(&mut self) {
        self.last_io = Some(Instant::now());
    }

    #[inline]
    fn poll_check(&mut self, cx: &mut Context) -> io::Result<()> {
        let timeout = match self.timeout {
            Some(timeout) => timeout,
            None => return Ok(()),
        };

        if self.last_io.is_none() {
            self.reset_elapsed();
        }

        match Pin::new(&mut self.timer).poll(cx) {
            Poll::Ready(()) => {
                let elapsed = self.last_io.unwrap().elapsed();
                if elapsed >= timeout {
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                } else {
                    let remaining_time = timeout - elapsed;
                    let deadline = Instant::now() + remaining_time;
                    self.timer.reset(deadline);
                    Ok(())
                }
            }
            Poll::Pending => Ok(()),
        }
    }
}

/// An `AsyncRead`er and `AsyncWrite`r which applies a timeout to I/O operations.
#[derive(Debug)]
pub struct TimeoutReaderWriter<R> {
    reader: R,
    state: TimeoutState,
}

impl<R> TimeoutReaderWriter<R>
where
    R: AsyncRead + Unpin,
{
    /// Returns a new `TimeoutReader` wrapping the specified reader.
    ///
    /// There is initially no timeout.
    pub fn new(reader: R) -> TimeoutReaderWriter<R> {
        TimeoutReaderWriter {
            reader,
            state: TimeoutState::new(),
        }
    }

    /// Returns the current read timeout.
    pub fn timeout(&self) -> Option<Duration> {
        self.state.timeout()
    }

    /// Sets the read timeout.
    ///
    /// This will reset any pending timeout.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.state.set_timeout(timeout);
    }

    /// Returns a shared reference to the inner reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Returns a mutable reference to the inner reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Consumes the `TimeoutReader`, returning the inner reader.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R> AsyncRead for TimeoutReaderWriter<R>
where
    R: AsyncRead + Unpin,
{
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        self.reader.prepare_uninitialized_buffer(buf)
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        let r = Pin::new(&mut self.reader).poll_read(cx, buf);
        match r {
            Poll::Pending => self.state.poll_check(cx)?,
            _ => self.state.reset_elapsed(),
        }
        r
    }

    fn poll_read_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, io::Error>>
    where
        B: BufMut,
    {
        let r = Pin::new(&mut self.reader).poll_read_buf(cx, buf);
        match r {
            Poll::Pending => self.state.poll_check(cx)?,
            _ => self.state.reset_elapsed(),
        }
        r
    }
}

impl<W> AsyncWrite for TimeoutReaderWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let r = Pin::new(&mut self.reader).poll_write(cx, buf);
        match r {
            Poll::Pending => self.state.poll_check(cx)?,
            _ => self.state.reset_elapsed(),
        }
        r
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        let r = Pin::new(&mut self.reader).poll_flush(cx);
        match r {
            Poll::Pending => self.state.poll_check(cx)?,
            _ => self.state.reset_elapsed(),
        }
        r
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        let r = Pin::new(&mut self.reader).poll_shutdown(cx);
        match r {
            Poll::Pending => self.state.poll_check(cx)?,
            _ => self.state.reset_elapsed(),
        }
        r
    }

    fn poll_write_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, io::Error>>
    where
        B: Buf,
    {
        let r = Pin::new(&mut self.reader).poll_write_buf(cx, buf);
        match r {
            Poll::Pending => self.state.poll_check(cx)?,
            _ => self.state.reset_elapsed(),
        }
        r
    }
}

/// A stream which applies read and write timeouts to an inner stream.
#[derive(Debug)]
pub struct TimeoutStream<S>(TimeoutReaderWriter<S>);

impl<S> TimeoutStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Returns a new `TimeoutStream` wrapping the specified stream.
    ///
    /// There is initially no read or write timeout.
    pub fn new(stream: S) -> TimeoutStream<S> {
        let reader = TimeoutReaderWriter::new(stream);
        TimeoutStream(reader)
    }

    /// Returns the current timeout.
    pub fn timeout(&self) -> Option<Duration> {
        self.0.timeout()
    }

    /// Sets the timeout.
    ///
    /// This will reset any pending timeout.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.0.set_timeout(timeout)
    }

    /// Returns a shared reference to the inner stream.
    pub fn get_ref(&self) -> &S {
        self.0.get_ref()
    }

    /// Returns a mutable reference to the inner stream.
    pub fn get_mut(&mut self) -> &mut S {
        self.0.get_mut()
    }

    /// Consumes the stream, returning the inner stream.
    pub fn into_inner(self) -> S {
        self.0.into_inner()
    }
}

impl<S> AsyncRead for TimeoutStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        self.0.prepare_uninitialized_buffer(buf)
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }

    fn poll_read_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, io::Error>>
    where
        B: BufMut,
    {
        Pin::new(&mut self.0).poll_read_buf(cx, buf)
    }
}

impl<S> AsyncWrite for TimeoutStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn poll_write_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, io::Error>>
    where
        B: Buf,
    {
        Pin::new(&mut self.0).poll_write_buf(cx, buf)
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;

    struct DelayStream(Delay);

    impl AsyncRead for DelayStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
            _buf: &mut [u8],
        ) -> Poll<Result<usize, io::Error>> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(()) => Poll::Ready(Ok(1)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl AsyncWrite for DelayStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(()) => Poll::Ready(Ok(buf.len())),
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn read_one<R>(mut reader: R) -> Result<usize, io::Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut buf: [u8; 1] = [0; 1];
        reader.read(&mut buf).await
    }

    #[tokio::test]
    async fn read_timeout() {
        let reader = DelayStream(delay_until(Instant::now() + Duration::from_millis(500)));
        let mut reader = TimeoutReaderWriter::new(reader);
        reader.set_timeout(Some(Duration::from_millis(100)));

        let r = read_one(reader).await;
        assert_eq!(r.err().unwrap().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn read_ok() {
        let reader = DelayStream(delay_until(Instant::now() + Duration::from_millis(100)));
        let mut reader = TimeoutReaderWriter::new(reader);
        reader.set_timeout(Some(Duration::from_millis(500)));

        read_one(reader).await.unwrap();
    }

    async fn write_one<W>(mut writer: W) -> Result<(), io::Error>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&[0]).await
    }

    #[tokio::test]
    async fn write_timeout() {
        let writer = DelayStream(delay_until(Instant::now() + Duration::from_millis(500)));
        let mut writer = TimeoutReaderWriter::new(writer);
        writer.set_timeout(Some(Duration::from_millis(100)));

        let r = write_one(writer).await;
        assert_eq!(r.err().unwrap().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn write_ok() {
        let writer = DelayStream(delay_until(Instant::now() + Duration::from_millis(100)));
        let mut writer = TimeoutReaderWriter::new(writer);
        writer.set_timeout(Some(Duration::from_millis(500)));

        write_one(writer).await.unwrap();
    }

    #[tokio::test]
    async fn tcp_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        thread::spawn(move || {
            let mut socket = listener.accept().unwrap().0;
            thread::sleep(Duration::from_millis(10));
            socket.write_all(b"f").unwrap();
            thread::sleep(Duration::from_millis(500));
            let _ = socket.write_all(b"f"); // this may hit an eof
        });

        let s = TcpStream::connect(&addr).await.unwrap();
        let mut s = TimeoutStream::new(s);
        s.set_timeout(Some(Duration::from_millis(100)));
        let _ = read_one(&mut s).await.unwrap();
        let r = read_one(&mut s).await;

        match r {
            Ok(_) => panic!("unexpected success"),
            Err(ref e) if e.kind() == io::ErrorKind::TimedOut => (),
            Err(e) => panic!("{:?}", e),
        }
    }
}
