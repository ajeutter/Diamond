# coding=utf-8

"""
Output the collected values through a Zer0MQ push channel to aggmon
"""

from Handler import Handler

try:
    import zmq
except ImportError:
    zmq = None

try:
    import ujson as json
except ImportError:
    try:
        import json
    except ImportError:
        json = None


class aggmonHandler (Handler):
    """
      Implements the abstract Handler class
      Sending data to a Zer0MQ push channel in a dict as expected by aggmon.
    """

    def __init__(self, config=None):

        """
          Create a new instance of aggmonHandler class
        """

        # Initialize Handler
        Handler.__init__(self, config)

        if not zmq:
            self.log.error('zmq import failed. Handler disabled')
            self.enabled = False
            return

        # Initialize Data
        self.context = None

        self.socket = None

        # Initialize Options
        self.collector = self.config['collector']

        # Create ZMQ PUSH socket and connect
        self._connect()

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(aggmonHandler, self).get_default_config_help()

        config.update({
            'collector': '',
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(aggmonHandler, self).get_default_config()

        config.update({
            'collector': 'tcp://127.0.0.1:5555'
        })

        return config

    def _connect(self):
        """
           Create PUSH socket and connect to collector
        """
        if not zmq:
            return
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.setsockopt(zmq.SNDHWM, 100000)
        self.socket.connect(self.collector)

    def __del__(self):
        """
          Destroy instance of the aggmonHandler class
        """
        if not zmq:
            return
        self.socket.disconnect(self.collector)
        self.socket.close()
        self.context.destroy()

    def process(self, metric):
        """
          Process a metric and send it to zmq pub socket
        """
        if not zmq or not json:
            return
        host = metric.host
        if host is None:
            host = metric.path.split(".")[1]
        timestamp = float(metric.timestamp)
        name = metric.getCollectorPath() + "." + metric.getMetricPath()
        value = metric.value
        # Send the data as json encoded dict
        jmetric = json.dumps({"N": name, "H": host, "V": value, "T": timestamp})
        #
        # We can risk to lose the data, therefore send it with NOBLOCK
        #
        self.socket.send(jmetric, flags=zmq.NOBLOCK)
