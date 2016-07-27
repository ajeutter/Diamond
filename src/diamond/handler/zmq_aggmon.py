# coding=utf-8

"""
Output the collected values through a Zer0MQ push channel to aggmon
"""

from Handler import Handler
import wdb
import time
import os

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
        self.socket.setsockopt(zmq.SNDHWM, 10000)
        self.socket.connect(self.collector)

    def __del__(self):
        """
          Destroy instance of the aggmonHandler class
        """
        pass
        #if not zmq:
        #    return
        #self.socket.disconnect(self.collector)
        #self.socket.close()
        #self.context.destroy()

    def process(self, metric):
        """
          Process a metric and send it to zmq pub socket
        """
        if not zmq or not json:
            return
        #
        # With multiprocessing.Process() the zmq context must be created
        # here and not in the __init__(). There we're in a different process!
        #
        if self.socket is None:
            self._connect()

        host = metric.host
        mp = metric.path.split(":")
        if host is None:
            host = metric.path.split(".")[1]
        if mp[0] == "servers"
            name = ".".join(mp[2:])
        else:
            name = metric.getCollectorPath() + "." + metric.getMetricPath()
        timestamp = float(metric.timestamp)
        value = metric.value
        # Send the data as json encoded dict
        jmetric = json.dumps({"N": name, "H": host, "V": value, "T": timestamp})
        #
        # We can risk to lose the data, therefore send it with NOBLOCK
        #
        try:
            self.socket.send(jmetric, flags=zmq.NOBLOCK)
        except Exception as e:
            pass

