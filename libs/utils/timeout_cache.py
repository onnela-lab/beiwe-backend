# source adapted from: https://gist.github.com/biblicabeebli/033fa16546a4f5ebe1ee401c1528fd9f

import inspect
from time import perf_counter
from typing import Callable
from types import MethodType, FunctionType

# Keys are the functions themselves, values are a tuple containing the timeout expiry
# of the cache entry, and the cached return value.
THE_TIMEOUT_CACHE = {}


# only use this on static or class methods of classes, and functions with _no arguments_.


def timeout_cache(seconds: int|float) -> Callable:
    if seconds <= 0:
        raise ValueError("Timeout cache decorator requires a non-zero timeout in seconds.")
    
    def check_timeout_cache(wrapped_func: FunctionType|MethodType):
        # you can't detect that a function is a method at-declaration-time because the classmethod
        # wrapper has not executed yet, and that's just how python works. It's hard to detect.
        
        # actually_the_func = wrapped_func.__func__ if is_method else wrapped_func
        # length = len(inspect.signature(actually_the_func).parameters) 
        # if is_method and length > 1:
        #     raise ValueError("timeout_cache is only supported on class methods with the single cls argument.")
        # elif length > 0:
        #     raise ValueError("timeout_cache is only supported on functions with no arguments.")
        
        # @wraps(wrapped_func)
        def cache_or_call(*args, **kwargs):
            # default (performant) case: look for cached function, check timeout.
            try:
                timeout, ret_val = THE_TIMEOUT_CACHE[wrapped_func]
                if perf_counter() < timeout:
                    return ret_val
            except KeyError:
                pass
            
            # slow case, cache miss: run function, cache the output, return output.
            ret_val = wrapped_func(*args, **kwargs)
            THE_TIMEOUT_CACHE[wrapped_func] = (perf_counter() + seconds, ret_val)
            return ret_val
        
        return cache_or_call
    return check_timeout_cache
