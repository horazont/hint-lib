import abc
import array
import asyncio
import logging
import bz2
import time
import typing

from datetime import timedelta

import aioxmpp

import hintlib.xso
import hintlib.utils


class RestartingTask:
    def __init__(self, coroutine_function,
                 logger=None,
                 loop=None):
        super().__init__()
        self._func = coroutine_function
        self._task = None
        self._should_run = False
        self.backoff = hintlib.utils.ExponentialBackOff()
        self._loop = loop or asyncio.get_event_loop()
        self._logger = logger or logging.getLogger(
            ".".join([__name__, type(self).__qualname__, str(id(self))]),
        )

    def start(self):
        self._should_run = True
        self.backoff.reset()
        self._ensure_state()

    def stop(self):
        self._should_run = False
        self._ensure_state()

    async def wait_for_termination(self):
        self.stop()
        if self._task is None:
            return
        try:
            await self._task
        except Exception as exc:
            pass

    def restart(self):
        if not self._should_run:
            return self.start()
        self.backoff.reset()
        if self._task is None:
            self._ensure_state()
        else:
            # cancel the task in order to force an immediate restart
            self._task.cancel()

    def _task_done(self, task):
        assert task is self._task
        try:
            try:
                result = task.result()
            except asyncio.CancelledError:
                self._logger.debug("task stopped after cancellation request")
                if self._should_run:
                    self._logger.info("restarting task immediately because"
                                      " the desired state is up")
                    self._loop.call_soon(self._ensure_state)
                return
            except BaseException as exc:
                delay = next(self.backoff)
                self._logger.error("task crashed! retrying in %s",
                                   delay, exc_info=True)
                self._loop.call_later(delay, self._ensure_state)
                return

            self._logger.info("task exited with result %r, not restarting",
                              result)
            self._should_run = False
        finally:
            self._task = None

    def _ensure_state(self):
        if self._task is None and self._should_run:
            # need to start task
            self._task = self._loop.create_task(self._func())
            self._task.add_done_callback(self._task_done)
        elif self._task is not None and not self._should_run:
            # need to stop task
            self._task.cancel()


class Buddies(aioxmpp.service.Service):
    ORDER_AFTER = [aioxmpp.RosterClient]

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.__buddies = []

    @property
    def buddies(self):
        return self.__buddies

    def set_buddies(self, buddies: typing.Mapping[
            aioxmpp.JID,
            typing.Set]):
        self._buddies = list(buddies)

    def load_buddies(self, buddies_cfg):
        self.__buddies = []
        for buddy in buddies_cfg:
            self.__buddies.append(
                (
                    aioxmpp.JID.fromstr(buddy["jid"]),
                    set(buddy.get("permissions", []))
                )
            )

    def get_by_permissions(self, keys):
        for jid, perms, *_ in self.__buddies:
            if "*" in perms or (perms & keys) == keys:
                yield jid

    @aioxmpp.service.depsignal(aioxmpp.Client, "on_stream_established")
    def on_stream_established(self):
        roster = self.dependencies[aioxmpp.RosterClient]
        for jid, *_ in self.__buddies:
            roster.approve(jid)
            roster.subscribe(jid)


class PeerLockService(aioxmpp.service.Service):
    ORDER_AFTER = [aioxmpp.PresenceClient]

    class PeerHandle:
        on_locked = aioxmpp.callbacks.Signal()
        on_unlocked = aioxmpp.callbacks.Signal()

        def __init__(self):
            self.lock_event = asyncio.Event()
            self.full_jid = None

        def _unlock(self):
            if self.full_jid is not None:
                self.on_unlocked()

            self.lock_event.clear()
            self.full_jid = None

        def _lock(self, full_jid):
            old_jid = self.full_jid
            self.full_jid = full_jid
            if old_jid != full_jid:
                self.on_locked(full_jid)
            self.lock_event.set()

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self._state = {}

    @aioxmpp.service.depsignal(aioxmpp.Client, "on_stream_destroyed")
    def _on_stream_destroyed(self, reason):
        for handle in self._state.values():
            handle.unlock()

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_available")
    def _on_available(self, full_jid, stanza):
        try:
            handle = self._state[stanza.from_.bare()]
        except KeyError:
            return

        self.logger.info("locked to JID %s", full_jid)
        handle._lock(full_jid)

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_unavailable")
    def _on_unavailable(self, full_jid, stanza):
        try:
            handle = self._state[stanza.from_.bare()]
        except KeyError:
            return

        self.logger.info("locked-to JID %s is offline, trying to lock to "
                         "another one", full_jid)

        resources = self.__presence.get_peer_resources(self.peer_jid).copy()
        resources.pop(stanza.from_.resource, None)
        if not resources:
            self.logger.debug("no more resources to lock to")
            handle._unlock()
            return

        # pick a "random" one
        next_resource = next(iter(resources.keys()))
        full_jid = full_jid.replace(resource=next_resource)
        self.logger.info("locked to %s in response to unavailable presence",
                         full_jid)

        handle._lock(full_jid)

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_bare_unavailable")
    def _on_bare_unavailable(self, stanza):
        try:
            handle = self._state[stanza.from_.bare()]
        except KeyError:
            return

        self.logger.info("%s went offline, unlocking", stanza.from_.bare())
        handle._unlock()

    def register_peer(self, peer_jid):
        if peer_jid.resource:
            raise ValueError("peer address must not be a full JID")

        try:
            return self._state[peer_jid]
        except KeyError:
            pass

        handle = self.PeerHandle()
        self._state[peer_jid] = handle
        try:
            resource = next(iter(
                self.dependencies[aioxmpp.PresenceClient].get_peer_resources(
                    peer_jid
                )
            ))
        except StopIteration:
            pass
        else:
            handle._lock(peer_jid.replace(resource=resource))
        return handle


class SenderService(aioxmpp.service.Service):
    ORDER_AFTER = [aioxmpp.PresenceClient]

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.__task_funs = []
        self.__locked_to = None
        self.__lock_event = asyncio.Event()
        self.__presence = self.dependencies[aioxmpp.PresenceClient]
        self.peer_jid = None
        self.__task = asyncio.ensure_future(
            self._supervisor()
        )
        self.__task.add_done_callback(
            self._supervisor_done,
        )

    def _task_done(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except:  # NOQA
            self.logger.exception(
                "task crashed"
            )

    def _supervisor_done(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except:  # NOQA
            self.logger.exception(
                "supervisor crashed"
            )

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_available")
    def _on_available(self, full_jid, stanza):
        if stanza.from_.bare() != self.peer_jid:
            return
        self.logger.info("locked to %s", full_jid)
        self.__locked_to = full_jid
        self.__lock_event.set()

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_unavailable")
    def _on_unavailable(self, full_jid, stanza):
        if stanza.from_ != self.__locked_to:
            return
        self.logger.info("locked-to JID %s is offline, trying to lock to "
                         "another one", full_jid)
        self.__locked_to = None
        self.__lock_event.clear()

        resources = self.__presence.get_peer_resources(self.peer_jid).copy()
        resources.pop(stanza.from_.resource, None)
        if not resources:
            self.logger.debug("no more resources to lock to")
            return

        # pick a "random" one
        next_resource = next(iter(resources.keys()))
        full_jid = full_jid.replace(resource=next_resource)
        self.logger.info("locked to %s in response to unavailable presence",
                         full_jid)

        self.__locked_to = full_jid
        self.__lock_event.set()

    @aioxmpp.service.depsignal(aioxmpp.PresenceClient, "on_bare_unavailable")
    def _on_bare_unavailable(self, stanza):
        if stanza.from_.bare() != self.peer_jid:
            return
        self.logger.info("%s went offline, unlocking", stanza.from_.bare())
        self.__locked_to = None
        self.__lock_event.clear()

    async def _wrapper(self, coro):
        try:
            try:
                await coro
            except asyncio.CancelledError:
                return
        except:  # NOQA
            await asyncio.sleep(1)
            raise
        await asyncio.sleep(1)

    async def _manage_tasks(self, tasks):
        self.__lock_event.clear()

        if self.__locked_to:
            # (re-)spawn tasks
            for fun in self.__task_funs:
                item = None
                try:
                    task = tasks[fun]
                except KeyError:
                    pass
                else:
                    if not task.done():
                        continue
                    try:
                        item = task.result()
                    except:  # NOQA
                        pass

                self.logger.debug(
                    "starting %s with %r", fun, item
                )
                task = asyncio.ensure_future(
                    self._wrapper(
                        fun(self.__locked_to, item)
                    )
                )
                task.add_done_callback(
                    self._task_done
                )
                tasks[fun] = task

        ev_fut = asyncio.ensure_future(
            self.__lock_event.wait()
        )
        await asyncio.wait(
            list(tasks.values()) + [ev_fut],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if ev_fut.done():
            ev_fut.result()
        else:
            ev_fut.cancel()

    async def _supervisor(self):
        tasks = {}
        try:
            while True:
                await self._manage_tasks(tasks)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                await task  # tasks are wrapped

    async def _shutdown(self):
        self.__task.cancel()
        try:
            await self.__task
        except asyncio.CancelledError:
            pass

    def add_task(self, coro_fun):
        self.__task_funs.append(coro_fun)
        self.__lock_event.set()


class SubmitterServiceMixin(metaclass=abc.ABCMeta):
    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self._queue = asyncio.Queue(maxsize=16)
        self._resubmit_delay = timedelta(seconds=1)
        sender = self.dependencies[SenderService]
        sender.add_task(self._impl)

    def _drop_item(self, item):
        pass

    @property
    def queue_size(self):
        return self._queue.maxsize

    @queue_size.setter
    def queue_size(self, value):
        new_queue = asyncio.Queue(value)
        all_items = [self._queue.get_nowait()
                     for i in range(self._queue.qsize())]

        if value is not None and value < len(all_items):
            to_drop = all_items[:len(all_items)-value]
            del all_items[:len(all_items)-value]
            for item in to_drop:
                self._drop_item(item)

        for item in all_items:
            new_queue.put_nowait(item)

        self._queue = new_queue

    @abc.abstractmethod
    def _compose_iq_payload(self, item):
        pass

    @property
    def resubmit_delay(self):
        return self._resubmit_delay

    @resubmit_delay.setter
    def resubmit_delay(self, value):
        self._resubmit_delay = value

    def _enqueue_dropping_old(self, item):
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            to_drop = self._queue.get_nowait()
            self.logger.warning("queue full, dropping %r", to_drop)
            self._drop_item(to_drop)
            self._queue.put_nowait(item)

    async def _submit_single(self, dest, item):
        self.logger.debug("composing IQ payload for %r", item)
        payload = self._compose_iq_payload(item)
        iq = aioxmpp.IQ(
            type_=aioxmpp.IQType.SET,
            to=dest,
            payload=payload
        )
        self.logger.debug("submitting %r", item)
        await self.client.send(iq)

    async def _impl(self, dest, cached_item=None):
        while True:
            if cached_item is None:
                item = await self._queue.get()
            else:
                item = cached_item
                cached_item = None

            try:
                await self._submit_single(dest, item)
            except aioxmpp.errors.XMPPError as exc:
                await asyncio.sleep(self._resubmit_delay.total_seconds())
                return item


class BatchSubmitterService(SubmitterServiceMixin,
                            aioxmpp.service.Service):
    ORDER_AFTER = [
        SenderService
    ]

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self._module_name = None

    @property
    def module_name(self):
        return self._module_name

    @module_name.setter
    def module_name(self, value):
        self._module_name = value

    def enqueue_batches(self, batches):
        self._enqueue_dropping_old(batches)

    def enqueue_batch(self, batch):
        self._enqueue_dropping_old([batch])

    def _compose_iq_payload(self, item):
        payload = hintlib.xso.Query()
        payload.sample_batches = hintlib.xso.SampleBatches()
        payload.sample_batches.module = self._module_name
        for t0, bare_path, samples in item:
            batch_xso = hintlib.xso.SampleBatch()
            batch_xso.timestamp = t0
            batch_xso.bare_path = str(bare_path)
            for subpart, value in samples.items():
                sample_xso = hintlib.xso.NumericSample()
                if subpart is not None:
                    sample_xso.subpart = subpart
                else:
                    sample_xso.subpart = None
                sample_xso.value = value
                batch_xso.samples.append(sample_xso)
            payload.sample_batches.batches.append(batch_xso)
        return payload


class StreamSubmitterService(SubmitterServiceMixin,
                             aioxmpp.service.Service):
    ORDER_AFTER = [
        SenderService
    ]

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        # the front queue can be sized infinitely
        self._front_queue = asyncio.Queue()
        self.__task = asyncio.ensure_future(self._compressor_task())
        self.__task.add_done_callback(self._handle_compressor_task_done)

    def _handle_compressor_task_done(self, task):
        if (task.cancelled() or
                isinstance(task.exception(), asyncio.CancelledError)):
            return

        self.logger.error("compressor task (%r) exited prematurely, "
                          "trying to re-start it", task)

        self.__task = asyncio.ensure_future(self._compressor_task())
        self.__task.add_done_callback(self._handle_compressor_task_done)

    def submit_block(self, block):
        self._front_queue.put_nowait(block)

    def _drop_item(self, item):
        _, _, _, _, _, _, handle = item
        handle.close()

    def _preprocess_item(self, item):
        path, t0, seq0, period, data, range_, handle = item

        bin_data = array.array("h", data).tobytes()
        ct0 = time.monotonic()
        bz2_data = bz2.compress(bin_data)
        ct1 = time.monotonic()
        self.logger.debug("sample compression took %.1f ms (%.0f%%)",
                          (ct1-ct0) * 1000,
                          (
                              (ct1-ct0) /
                              (period*len(data)).total_seconds()
                          ) * 100)
        return path, t0, seq0, period, bz2_data, range_, handle

    async def _compressor_task(self):
        loop = asyncio.get_event_loop()
        while True:
            item = await self._front_queue.get()
            try:
                processed_item = await loop.run_in_executor(
                    None,
                    self._preprocess_item,
                    item,
                )
            except asyncio.CancelledError:
                # just break here
                break
            except:  # NOQA
                self.logger.error("failed to preprocess item",
                                  exc_info=True)
                self._drop_item(item)
                raise
            self._enqueue_dropping_old(processed_item)

    def _compose_iq_payload(self, item):
        path, t0, seq0, period, bz2_data, range_, handle = item
        payload = hintlib.xso.Query()
        payload.stream = hintlib.xso.Stream()
        payload.stream.path = str(path)
        payload.stream.t0 = t0
        payload.stream.period = round(period.total_seconds() * 1e6)
        payload.stream.sample_type = "h"
        payload.stream.data = bz2_data
        payload.stream.seq0 = seq0
        payload.stream.range_ = range_
        return payload

    async def _submit_single(self, dest, item):
        try:
            await super()._submit_single(dest, item)
        except aioxmpp.errors.XMPPError:
            raise
        else:
            # only close the handle on non-XMPPError errors
            self._drop_item(item)
