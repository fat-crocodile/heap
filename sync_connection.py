from pika import SelectConnection
from pika.channel import Channel
import functools

class SyncSelectConnection(SelectConnection):
    # sync init, wait untill connection established
    def __init__(self, *args, **nargs):
        x = []
        def on_open(connection):
            x.append(0)

        nargs['on_open_callback'] = on_open
        nargs['on_close_callback'] = self._my_on_close

        SelectConnection.__init__(self, *args, **nargs)

        while not x:
            self.ioloop.poller.poll()

    # sync channel creation, wait untill channel appears
    def channel(self):
        x = []
        def on_open(ch):
            x.append(ch)

        SelectConnection.channel(self, on_open)

        while not x:
            self.ioloop.poller.poll()

        return x[0]

    # owerride standard method _create_channel, return SyncChannel instead of pika.Channel
    def _create_channel(self, channel_number, on_open_callback):
        return SyncChannel(self, channel_number, on_open_callback)

    # raise exception on unexpected connection close
    @staticmethod
    def _my_on_close(conn, code, text):
        if code != 0:
            raise Exception('Connection %s closed (%s) %s' % (conn.params, code, text))

    # adjust interface to BlockingConnection
    def process_data_events(self):
        self.ioloop.poller.poll()

# can syncronize any channel method
def sync_channel(proc):
    @functools.wraps(proc)
    def f(self, *argc, **nargs):
        x = []
        def on_open(method):
            x.append(0)

        proc(self, on_open, *argc, **nargs)

        while not x:
            self.connection.ioloop.poller.poll()

    return f

class SyncChannel(Channel):
    # just add on_close, callback syncronization in SyncSelectConnection.channel
    def __init__(self, *args, **nargs):
        Channel.__init__(self, *args, **nargs)
        self.add_on_close_callback(self._my_on_close)

    @sync_channel
    def queue_declare(self, *args, **nargs):
        return Channel.queue_declare(self, *args, **nargs)

    @sync_channel
    def exchange_declare(self, *args, **nargs):
        return Channel.exchange_declare(self, *args, **nargs)

    @sync_channel
    def queue_bind(self, *args, **nargs):
        return Channel.queue_bind(self, *args, **nargs)

    @sync_channel
    def exchange_bind(self, *args, **nargs):
        return Channel.exchange_bind(self, *args, **nargs)

    @sync_channel
    def queue_delete(self, *args, **nargs):
        return Channel.queue_delete(self, *args, **nargs)

    @sync_channel
    def exchange_delete(self, *args, **nargs):
        return Channel.exchange_delete(self, *args, **nargs)

    @staticmethod
    def _my_on_close(ch, code, text):
        if code != 0:
            raise Exception('Channel %d belongs to connection %s closed (%s) %s' % (ch.channel_number, ch.connection.params, code, text))

    # adjust channel interface to BlockingChannel
    def stop_consuming(self):
        """Sends off the Basic.Cancel to let RabbitMQ know to stop consuming and
        sets our internal state to exit out of the basic_consume.
        """
        for consumer_tag in self._consumers.keys():
            self.basic_cancel(consumer_tag)

