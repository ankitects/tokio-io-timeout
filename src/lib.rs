//! Tokio wrappers which apply timeouts to IO operations.
//!
//! These timeouts are analogous to the read and write timeouts on traditional blocking sockets. A timeout countdown is
//! initiated when a read/write operation returns [`Poll::Pending`]. If a read/write does not return successfully before
//! the countdown expires, an [`io::Error`] with a kind of [`TimedOut`](io::ErrorKind::TimedOut) is returned.
#![doc(html_root_url = "https://docs.rs/tokio-io-timeout/1")]
#![warn(missing_docs)]

use pin_project_lite::pin_project;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{sleep_until, Instant, Sleep};

pin_project! {
    #[derive(Debug)]
    struct TimeoutState {
        timeout: Option<Duration>,
        #[pin]
        timer: Sleep,
        last_io: Option<Instant>,
    }
}

impl TimeoutState {
    #[inline]
    fn new() -> TimeoutState {
        TimeoutState {
            timeout: None,
            timer: sleep_until(Instant::now()),
            last_io: None,
        }
    }

    #[inline]
    fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    #[inline]
    fn set_timeout(&mut self, timeout: Option<Duration>) {
        // since this takes &mut self, we can't yet be active
        self.timeout = timeout;
    }

    #[inline]
    fn set_timeout_pinned(mut self: Pin<&mut Self>, timeout: Option<Duration>) {
        *self.as_mut().project().timeout = timeout;
        *self.as_mut().project().last_io = None;
    }

    #[inline]
    fn reset_elapsed(mut self: Pin<&mut Self>) {
        *self.as_mut().project().last_io = Some(Instant::now());
    }

    #[inline]
    fn poll_check(self: Pin<&mut Self>, cx: &mut Context<'_>) -> io::Result<()> {
        let mut this = self.project();

        let timeout = match *this.timeout {
            Some(timeout) => timeout,
            None => return Ok(()),
        };

        if this.last_io.is_none() {
            *this.last_io = Some(Instant::now());
        }

        match this.timer.as_mut().poll(cx) {
            Poll::Ready(()) => {
                let elapsed = this.last_io.unwrap().elapsed();
                if elapsed >= timeout {
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                } else {
                    let remaining_time = timeout - elapsed;
                    let deadline = Instant::now() + remaining_time;
                    this.timer.as_mut().reset(deadline);
                    Ok(())
                }
            }
            Poll::Pending => Ok(()),
        }
    }
}

pin_project! {
    /// An `AsyncRead`er and `AsyncWrite`r which applies a timeout to I/O operations.
    #[derive(Debug)]
    pub struct TimeoutReaderWriter<R> {
        #[pin]
        reader: R,
        #[pin]
        state: TimeoutState,
    }
}

impl<R> TimeoutReaderWriter<R>
where
    R: AsyncRead,
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
    /// This can only be used before the reader is pinned; use [`set_timeout_pinned`](Self::set_timeout_pinned)
    /// otherwise.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.state.set_timeout(timeout);
    }

    /// Sets the read timeout.
    ///
    /// This will reset any pending timeout. Use [`set_timeout`](Self::set_timeout) instead if the reader is not yet
    /// pinned.
    pub fn set_timeout_pinned(self: Pin<&mut Self>, timeout: Option<Duration>) {
        self.project().state.set_timeout_pinned(timeout);
    }

    /// Returns a shared reference to the inner reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// Returns a mutable reference to the inner reader.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Returns a pinned mutable reference to the inner reader.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut R> {
        self.project().reader
    }

    /// Consumes the `TimeoutReader`, returning the inner reader.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R> AsyncRead for TimeoutReaderWriter<R>
where
    R: AsyncRead,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        let r = this.reader.poll_read(cx, buf);
        match r {
            Poll::Pending => this.state.poll_check(cx)?,
            _ => this.state.reset_elapsed(),
        }
        r
    }
}

impl<W> AsyncWrite for TimeoutReaderWriter<W>
where
    W: AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        let r = this.reader.poll_write(cx, buf);
        match r {
            Poll::Pending => this.state.poll_check(cx)?,
            _ => this.state.reset_elapsed(),
        }
        r
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        let r = this.reader.poll_flush(cx);
        match r {
            Poll::Pending => this.state.poll_check(cx)?,
            _ => this.state.reset_elapsed(),
        }
        r
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        let r = this.reader.poll_shutdown(cx);
        match r {
            Poll::Pending => this.state.poll_check(cx)?,
            _ => this.state.reset_elapsed(),
        }
        r
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        let r = this.reader.poll_write_vectored(cx, bufs);
        match r {
            Poll::Pending => this.state.poll_check(cx)?,
            _ => this.state.reset_elapsed(),
        }
        r
    }

    fn is_write_vectored(&self) -> bool {
        self.reader.is_write_vectored()
    }
}

pin_project! {
    /// A stream which applies read and write timeouts to an inner stream.
    #[derive(Debug)]
    pub struct TimeoutStream<S> {
        #[pin]
        stream: TimeoutReaderWriter<S>
    }
}

impl<S> TimeoutStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    /// Returns a new `TimeoutStream` wrapping the specified stream.
    ///
    /// There is initially no read or write timeout.
    pub fn new(stream: S) -> TimeoutStream<S> {
        let reader  = TimeoutReaderWriter::new(stream);
        TimeoutStream { stream: reader }
    }

    /// Returns the current timeout.
    pub fn timeout(&self) -> Option<Duration> {
        self.stream.timeout()
    }

    /// Sets the timeout.
    ///
    /// This can only be used before the stream is pinned; use
    /// [`set_read_timeout_pinned`](Self::set_read_timeout_pinned) otherwise.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.stream.set_timeout(timeout)
    }

    /// Sets the read timeout.
    ///
    /// This will reset any pending read timeout. Use [`set_read_timeout`](Self::set_read_timeout) instead if the stream
    /// has not yet been pinned.
    pub fn set_timeout_pinned(self: Pin<&mut Self>, timeout: Option<Duration>) {
        self.project().stream.set_timeout_pinned(timeout)
    }

    /// Returns a shared reference to the inner stream.
    pub fn get_ref(&self) -> &S {
        self.stream.get_ref()
    }

    /// Returns a mutable reference to the inner stream.
    pub fn get_mut(&mut self) -> &mut S {
        self.stream.get_mut()
    }

    /// Returns a pinned mutable reference to the inner stream.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut S> {
        self.project().stream.get_pin_mut()
    }

    /// Consumes the stream, returning the inner stream.
    pub fn into_inner(self) -> S {
        self.stream.into_inner()
    }
}

impl<S> AsyncRead for TimeoutStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), io::Error>> {
        self.project().stream.poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for TimeoutStream<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.project().stream.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.project().stream.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::pin;

    pin_project! {
        struct DelayStream {
            #[pin]
            sleep: Sleep,
        }
    }

    impl DelayStream {
        fn new(until: Instant) -> Self {
            DelayStream {
                sleep: sleep_until(until),
            }
        }
    }

    impl AsyncRead for DelayStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context,
            _buf: &mut ReadBuf,
        ) -> Poll<Result<(), io::Error>> {
            match self.project().sleep.poll(cx) {
                Poll::Ready(()) => Poll::Ready(Ok(())),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl AsyncWrite for DelayStream {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            match self.project().sleep.poll(cx) {
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

    #[tokio::test]
    async fn read_timeout() {
        let reader = DelayStream::new(Instant::now() + Duration::from_millis(500));
        let mut reader = TimeoutReaderWriter::new(reader);
        reader.set_timeout(Some(Duration::from_millis(100)));
        pin!(reader);

        let r = reader.read(&mut [0]).await;
        assert_eq!(r.err().unwrap().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn read_ok() {
        let reader = DelayStream::new(Instant::now() + Duration::from_millis(100));
        let mut reader = TimeoutReaderWriter::new(reader);
        reader.set_timeout(Some(Duration::from_millis(500)));
        pin!(reader);

        reader.read(&mut [0]).await.unwrap();
    }

    #[tokio::test]
    async fn write_timeout() {
        let writer = DelayStream::new(Instant::now() + Duration::from_millis(500));
        let mut writer = TimeoutReaderWriter::new(writer);
        writer.set_timeout(Some(Duration::from_millis(100)));
        pin!(writer);

        let r = writer.write(&[0]).await;
        assert_eq!(r.err().unwrap().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn write_ok() {
        let writer = DelayStream::new(Instant::now() + Duration::from_millis(100));
        let mut writer = TimeoutReaderWriter::new(writer);
        writer.set_timeout(Some(Duration::from_millis(500)));
        pin!(writer);

        writer.write(&[0]).await.unwrap();
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
        pin!(s);
        s.read(&mut [0]).await.unwrap();
        let r = s.read(&mut [0]).await;

        match r {
            Ok(_) => panic!("unexpected success"),
            Err(ref e) if e.kind() == io::ErrorKind::TimedOut => (),
            Err(e) => panic!("{:?}", e),
        }
    }
}
