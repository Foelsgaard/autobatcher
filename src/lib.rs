use futures::future::Future;
use futures::stream::Stream;
use futures::task::{waker_ref, ArcWake, Poll, Waker};
use slab::Slab;
use std::cell::UnsafeCell;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub trait Context {
    type Request;
    type Response;
    type Fut: Future<Output = Vec<Self::Response>>;

    fn call(&mut self, requests: &[Self::Request]) -> Self::Fut;
}

pub struct Batcher<SIG, CTX: Context> {
    inner: Arc<Inner<SIG, CTX>>,
}

pub struct BatchingCall<SIG, CTX: Context> {
    request: Option<CTX::Request>,
    index: usize,
    batch_index: bool,
    inner: Option<Arc<Inner<SIG, CTX>>>,
    waker_key: usize,
}

impl<SIG, CTX: Context> Clone for Batcher<SIG, CTX> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<SIG, CTX: Context> Batcher<SIG, CTX> {
    pub fn new(signal: SIG, context: CTX) -> Self {
        Self {
            inner: Arc::new(Inner {
                notifier: Arc::new(Notifier {
                    wakers: Mutex::new(Slab::new()),
                }),
                context: UnsafeCell::new(context),

                signal: UnsafeCell::new(signal),
                signal_state: AtomicUsize::new(IDLE),

                call: UnsafeCell::new(None),
                call_buffer: UnsafeCell::new(vec![]),
                call_state: AtomicUsize::new(OUTBOX_EMPTY),

                switch: AtomicBool::new(false),
                inbox: Mutex::new(vec![]),
                outbox: Mutex::new(Slab::new()),
            }),
        }
    }

    pub fn call(&self, request: CTX::Request) -> BatchingCall<SIG, CTX> {
        BatchingCall {
            request: Some(request),
            index: 0,
            batch_index: false,
            inner: Some(Arc::clone(&self.inner)),
            waker_key: NULL_WAKER_KEY,
        }
    }
}

struct Inner<SIG, CTX: Context> {
    notifier: Arc<Notifier>,

    context: UnsafeCell<CTX>,

    signal: UnsafeCell<SIG>,
    signal_state: AtomicUsize,

    call: UnsafeCell<Option<CTX::Fut>>,
    call_buffer: UnsafeCell<Vec<CTX::Request>>,
    call_state: AtomicUsize,

    switch: AtomicBool,
    inbox: Mutex<Vec<CTX::Request>>,
    outbox: Mutex<Slab<CTX::Response>>,
}

unsafe impl<SIG, CTX: Context> Send for Inner<SIG, CTX>
where
    CTX: Send,
    CTX::Fut: Send,
    <CTX::Fut as Future>::Output: Send + Sync,
{
}
unsafe impl<SIG, CTX: Context> Sync for Inner<SIG, CTX>
where
    CTX: Send,
    CTX::Fut: Send,
    <CTX::Fut as Future>::Output: Send + Sync,
{
}

struct Notifier {
    wakers: Mutex<Slab<Option<Waker>>>,
}

const IDLE: usize = 0;
const POLLING: usize = 1;
const COMPLETE: usize = 2;
const OUTBOX_EMPTY: usize = 3;
const POISONED: usize = 4;

const NULL_WAKER_KEY: usize = usize::max_value();

impl<SIG, CTX: Context> Unpin for BatchingCall<SIG, CTX> {}

impl<SIG: Stream, CTX: Context> Future for BatchingCall<SIG, CTX> {
    type Output = CTX::Response;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<<Self as futures::Future>::Output> {
        use Ordering::*;

        let this = &mut *self;
        let inner = this
            .inner
            .take()
            .expect("BatchingCall polled after completion");
        inner.notifier.record_waker(&mut this.waker_key, cx);

        if let Some(request) = this.request.take() {
            let mut inbox = inner.inbox.lock().unwrap();
            this.index = inbox.len();
            this.batch_index = inner.switch.load(SeqCst);
            inbox.push(request);
        }

        struct Reset<'a>(&'a AtomicUsize);

        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                use std::thread;

                if thread::panicking() {
                    self.0.store(POISONED, SeqCst);
                }
            }
        }

        let common_waker = waker_ref(&inner.notifier);

        if this.batch_index != inner.switch.load(SeqCst) {
            match inner
                .call_state
                .compare_exchange(IDLE, POLLING, SeqCst, SeqCst)
                .unwrap_or_else(|e| e)
            {
                IDLE => {}
                POLLING => {
                    this.inner = Some(inner);
                    return Poll::Pending;
                }
                COMPLETE => {
                    let mut outbox = inner.outbox.lock().unwrap();
                    let value = outbox.remove(this.index);
                    if outbox.is_empty() {
                        inner.call_state.store(OUTBOX_EMPTY, SeqCst); // COMPLETE -> OUTBOX_EMPTY
                    }

                    inner.notifier.forget_waker(&mut this.waker_key);
                    return Poll::Ready(value);
                }
                POISONED => panic!("inner future panicked during poll"),
                _ => unreachable!(),
            }

            let _reset = Reset(&inner.call_state);

            let maybe_call = unsafe { &mut *inner.call.get() };

            match maybe_call {
                Some(call) => unsafe {
                    match Pin::new_unchecked(call).poll(cx) {
                        Poll::Ready(responses) => {
                            let mut outbox = inner.outbox.lock().unwrap();
                            *outbox = responses.into_iter().enumerate().collect();

                            let value = outbox.remove(this.index);

                            if outbox.is_empty() {
                                inner.call_state.store(OUTBOX_EMPTY, SeqCst); // POLLING -> OUTBOX_EMPTY
                            } else {
                                inner.call_state.store(COMPLETE, SeqCst); // POLLING -> COMPLETE
                            }

                            inner.notifier.forget_waker(&mut this.waker_key);
                            common_waker.wake_by_ref();

                            return Poll::Ready(value);
                        }
                        Poll::Pending => {}
                    }
                },
                None => {}
            };

            inner.call_state.store(IDLE, SeqCst); // POLLING -> IDLE
            drop(_reset);
            this.inner = Some(inner);
            Poll::Pending
        } else {
            match inner
                .signal_state
                .compare_exchange(IDLE, POLLING, SeqCst, SeqCst)
                .unwrap_or_else(|e| e)
            {
                IDLE => {}
                POLLING => {
                    this.inner = Some(inner);
                    return Poll::Pending;
                }
                POISONED => panic!("inner future panicked during poll"),
                _ => unreachable!(),
            }
            let _reset = Reset(&inner.signal_state);

            let signal = unsafe { Pin::new_unchecked(&mut *inner.signal.get()) };

            if inner.call_state.load(SeqCst) == OUTBOX_EMPTY && signal.poll_next(cx).is_ready() {
                let call = unsafe { &mut *inner.call.get() };
                let call_buffer = unsafe { &mut *inner.call_buffer.get() };
                let context = unsafe { &mut *inner.context.get() };

                let inbox = &mut *inner.inbox.lock().unwrap();

                call_buffer.clear();
                std::mem::swap(inbox, call_buffer);
                *call = Some(context.call(call_buffer));

                inner.call_state.store(IDLE, SeqCst); // OUTBOX_EMPTY -> IDLE
                inner.switch.fetch_xor(true, SeqCst);

                common_waker.wake_by_ref();
            }

            inner.signal_state.store(IDLE, SeqCst); // POLLING -> IDLE
            drop(_reset);
            this.inner = Some(inner);
            Poll::Pending
        }
    }
}

impl Notifier {
    /// Registers the current task to receive a wakeup when we are awoken.
    fn record_waker(&self, waker_key: &mut usize, cx: &mut std::task::Context<'_>) {
        let wakers = &mut *self.wakers.lock().unwrap();

        let new_waker = cx.waker();

        if *waker_key == NULL_WAKER_KEY {
            *waker_key = wakers.insert(Some(new_waker.clone()));
        } else {
            match wakers[*waker_key] {
                Some(ref old_waker) if new_waker.will_wake(old_waker) => {}
                ref mut slot => *slot = Some(new_waker.clone()),
            }
        }
    }

    fn forget_waker(&self, waker_key: &mut usize) {
        let wakers = &mut *self.wakers.lock().unwrap();

        if *waker_key != NULL_WAKER_KEY {
            wakers.remove(*waker_key);
            *waker_key = NULL_WAKER_KEY;
        }
    }
}

impl<SIG, CTX: Context> Drop for BatchingCall<SIG, CTX> {
    fn drop(&mut self) {
        if self.waker_key != NULL_WAKER_KEY {
            if let Some(ref inner) = self.inner {
                if let Ok(mut wakers) = inner.notifier.wakers.lock() {
                    wakers.remove(self.waker_key);
                }
            }
        }
    }
}

impl ArcWake for Notifier {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let wakers = &mut *arc_self.wakers.lock().unwrap();
        for (_key, opt_waker) in wakers {
            if let Some(waker) = opt_waker.take() {
                waker.wake();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use futures::{future, stream};
    use futures::stream::{FuturesOrdered, StreamExt};
    use tokio;
    use tokio_stream::wrappers::IntervalStream;

    struct TestContext;

    impl Context<usize, usize> for TestContext {
        type Fut = future::Ready<Vec<usize>>;

        fn call(&mut self, requests: &[usize]) -> Self::Fut {
            future::ready(requests.to_vec())
        }
    }

    #[tokio::test]
    async fn test_order_sync() {
        let context = TestContext;
        let signal = IntervalStream::new(tokio::time::interval(Duration::from_micros(500)));
        let batcher = Batcher::new(signal, context);

        let mut tasks = FuturesOrdered::new();

        const N: usize = 10000;

        let requests: Vec<usize> = (0..N).collect();

        for n in requests.iter() {
            tasks.push(batcher.call(*n));
        }

        let responses: Vec<usize> = tasks.collect().await;

        assert_eq!(requests, responses);
    }

    #[tokio::test]
    async fn test_order_async() {
        let context = TestContext;
        let signal = IntervalStream::new(tokio::time::interval(Duration::from_micros(500)));
        let batcher = Batcher::new(signal, context);

        const N: usize = 10000;

        let requests: Vec<usize> = (0..N).collect();
        let mut tasks = FuturesOrdered::new();

        for n in requests.iter() {
            tasks.push(tokio::spawn(batcher.call(*n)));
        }

        let responses: Vec<usize> = tasks.flat_map(stream::iter).collect().await;

        assert_eq!(requests, responses);
    }
}
