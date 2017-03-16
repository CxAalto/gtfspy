"""
An abstract class defining some requirements for the algorithms inherited by this class.
"""
from abc import ABCMeta, abstractmethod

import time
from gtfspy.routing.util import timeit


class AbstractRoutingAlgorithm:
    __metaclass__ = ABCMeta

    def __init__(self):
        self._run_time = None
        self._has_run = None

    @abstractmethod
    def _run(self):
        pass

    @timeit
    def run(self):
        if self._has_run:
            raise RuntimeError("Algorithm has already run, please initialize a new algorithm")
        start_time = time.time()
        self._run()
        end_time = time.time()
        self._run_time = end_time - start_time
        self._has_run = True

    def get_run_time(self):
        """
        Returns
        -------
        run_time: float
            running time of the algorithm in seconds
        """
        assert self._has_run
        return self._run_time
