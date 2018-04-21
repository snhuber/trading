"""This module does blah blah."""

class MyClass(object):
    """a class"""
    def __init__(self):
        pass


def a1_func(fd: int = 2) -> object:
    """does something"""
    print("running a1_func()", fd)
    return None


def complex_number(real: float = 0.0, imag: float = 0.0, **kwargs) -> complex:
    r"""Fetches and returns this thing

    :param real:
        The first parameter
    :type real: ``float``
    :param imag:
        The second parameter
    :type imag: ``float``
    :param \**kwargs:
        See below

    :Keyword Arguments:
        * *extra* (``list``) --
          Extra stuff
        * *supplement* (``dict``) --
          Additional content

    """
    if imag == 0.0 and real == 0.0:
        return complex(real, imag)
