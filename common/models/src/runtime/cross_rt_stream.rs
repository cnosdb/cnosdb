//! Fork influxdb_iox/iox_query/src/exec/cross_rt_stream.rs
//!
//! Tooling to pull [`Stream`]s from one tokio runtime into another.
//!
//! This is critical so that CPU heavy loads are not run on the same runtime as IO handling
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::error::DataFusionError;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;

use super::executor::{self, DedicatedExecutor};

/// [`Stream`] that is calculated by one tokio runtime but can safely be pulled from another w/o stalling (esp. when the
/// calculating runtime is CPU-blocked).
pub struct CrossRtStream<T> {
    /// Future that drives the underlying stream.
    ///
    /// This is actually wrapped into [`DedicatedExecutor::spawn`] so it can be safely polled by the receiving runtime.
    driver: BoxFuture<'static, ()>,

    /// Flags if the [driver](Self::driver) returned [`Poll::Ready`].
    driver_ready: bool,

    /// Receiving stream.
    ///
    /// This one can be polled from the receiving runtime.
    inner: ReceiverStream<T>,

    /// Signals that [`inner`](Self::inner) finished.
    ///
    /// Note that we must also drive the [driver](Self::driver) even when the stream finished to allow proper state clean-ups.
    inner_done: bool,
}

impl<T> std::fmt::Debug for CrossRtStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossRtStream")
            .field("driver", &"...")
            .field("driver_ready", &self.driver_ready)
            .field("inner", &"...")
            .field("inner_done", &self.inner_done)
            .finish()
    }
}

impl<T> CrossRtStream<T> {
    /// Create new stream by producing a future that sends its state to the given [`Sender`].
    ///
    /// This is an internal method. `f` should always be wrapped into [`DedicatedExecutor::spawn`] (except for testing purposes).
    fn new_with_tx<F, Fut>(f: F) -> Self
    where
        F: FnOnce(Sender<T>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = channel(1);
        let driver = f(tx).boxed();
        Self {
            driver,
            driver_ready: false,
            inner: ReceiverStream::new(rx),
            inner_done: false,
        }
    }
}

impl<X, E> CrossRtStream<Result<X, E>>
where
    X: Send + 'static,
    E: Send + 'static,
{
    /// Create new stream based on an existing stream that transports [`Result`]s.
    ///
    /// Also receives an executor that actually executes the underlying stream as well as a converter that convets
    /// [`executor::Error`] to the error type of the stream (so we can send potential crashes/panics).
    fn new_with_error_stream<S, C>(stream: S, exec: DedicatedExecutor, converter: C) -> Self
    where
        S: Stream<Item = Result<X, E>> + Send + 'static,
        C: Fn(executor::Error) -> E + Send + 'static,
    {
        Self::new_with_tx(|tx| {
            // future to be run in the other runtime
            let tx_captured = tx.clone();
            let fut = async move {
                tokio::pin!(stream);

                while let Some(res) = stream.next().await {
                    if tx_captured.send(res).await.is_err() {
                        // receiver gone
                        return;
                    }
                }
            };

            // future for this runtime (likely the tokio/tonic/web driver)
            async move {
                if let Err(e) = exec.spawn(fut).await {
                    let e = converter(e);

                    // last message, so we don't care about the receiver side
                    tx.send(Err(e)).await.ok();
                }
            }
        })
    }
}

impl<X> CrossRtStream<Result<X, DataFusionError>>
where
    X: Send + 'static,
{
    /// Create new stream based on an existing stream that transports [`Result`]s w/ [`DataFusionError`]s.
    ///
    /// Also receives an executor that actually executes the underlying stream.
    pub fn new_with_df_error_stream<S>(stream: S, exec: DedicatedExecutor) -> Self
    where
        S: Stream<Item = Result<X, DataFusionError>> + Send + 'static,
    {
        Self::new_with_error_stream(stream, exec, |e| {
            DataFusionError::Context(
                "Join Error (panic)".to_string(),
                Box::new(DataFusionError::External(e.into())),
            )
        })
    }
}

impl<T> Stream for CrossRtStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if !this.driver_ready {
            let res = this.driver.poll_unpin(cx);

            if res.is_ready() {
                this.driver_ready = true;
            }
        }

        if this.inner_done {
            if this.driver_ready {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        } else {
            match ready!(this.inner.poll_next_unpin(cx)) {
                None => {
                    this.inner_done = true;
                    if this.driver_ready {
                        Poll::Ready(None)
                    } else {
                        Poll::Pending
                    }
                }
                Some(x) => Poll::Ready(Some(x)),
            }
        }
    }
}
