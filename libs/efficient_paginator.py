import csv
from collections.abc import Generator
from time import perf_counter
from typing import Any

from django.db.models import QuerySet
from orjson import dumps as orjson_dumps

from libs.streaming_io import StreamingStringsIO


class EfficientQueryPaginator:
    """ Enables database queries that maximize specific efficiencies to enable streaming output.
    - Memory efficiency:
        - Large queries crash with MemoryErrors and take up global system resources. It dumb.
        - Django has a query cache. We have to bypass it to avoid holding entire queries in memory.
        - Usage of .iterator() bypasses the cache, but you still need to handle it's output and
          avoid manually storing all the returns at one time.
    - Time-To-First-Row:
        - Streaming requires we need to return a first datapoint as quickly as possible.
        - Queries that don't need to assemble complex returns do so quickly
        - Iterator() queries start returning data quickly.
        - We can maximize both by splitting the query into two parts. Run the filters but return
          only primary keys, then runa query on the primary keys to get the actual data.
    - Database Contention:
        - long-running non-iterator queries can lock the database. Django has a per-process single
          database connection architecture. It... causes problems. This class helps ensure long
          running streaming tasks don't hog a database connection.
    - Overhead:
        - There is a tradeoff, those tradeoffs result in slower time-to-completion, but...
        - Some queries are slow because they are memory-bound _at the database_, a situation where
          this class ends up being faster.
        - Time-to-first-data is functionally faster under many scenarios. Think time-to-first-item
          on a list on a web page.
    - Restrictions:
        - You shouldn't use full database objects when you want efficiency at scale. instantiation
          of those objects has potentially substantial overhead.
        - Order-By will probably eliminate gains of time-to-first-row.
    - Features:
        - Automatic flat=True for values_list queries with a single field! Hallelujah!
        - Pagination and iteration both supported.
        - Stream out bytes for a JSON list using a highly optimized json library.
        - Stream out bytes for a CSV file.
    """
    
    def __init__(
        self,
        filtered_query: QuerySet,
        page_size: int,
        limit: int = 0,
        values: list[str] = None,
        values_list: list[str] = None,
        flat=True,
        verbose=False,
    ):
        if (not values and not values_list) or (values and values_list):  # Not. Negotiable.
            raise Exception("exactly one of values or values_list must be provided")
        
        if page_size > 50_000:
            raise Exception("page_size must be less than 50,000, there's a fundamental 65k limit.")
        
        self.page_size = page_size
        self.pk_query = filtered_query.values_list("pk", flat=True)
        self.field_names = values or values_list
        
        self.doing_values_list = bool(values_list)
        
        # can't filter after a limit (done in pagination), solution is to limit the pk query.
        if limit:
            self.pk_query = self.pk_query[:limit]
        
        # pass values params intelligently
        
        if values:
            self.value_query = filtered_query.values(*self.field_names)
        elif values_list:
            self.value_query = filtered_query.values_list(
                *values_list, flat=flat and len(self.field_names) == 1  # intelligently handle flat=True
            )
            self.values_list = values_list
        
        if verbose:
            print("EfficientQueryPaginator __init__ end")
            print(f"EfficientQueryPaginator pk_query: {self.pk_query.explain()}")
            print(f"EfficientQueryPaginator value_query: {self.value_query.explain()}")
        self.verbose = verbose
    
    # uh this is kinda not tested and had a bug...
    def __iter__(self) -> Generator[Any, None, None]:
        """ Grab a page of PKs, the results via iteration. (_Might_ have better memory usage.) """
        pks = []
        if self.verbose:
            print("EfficientQueryPaginator __iter__ start")
        
        start = perf_counter()
        for count, pk in enumerate(self.pk_query.iterator(chunk_size=self.page_size), start=1):
            pks.append(pk)
            if count % self.page_size == 0:
                if self.verbose:
                    if start:
                        elapsed = perf_counter() - start
                        print(f"EfficientQueryPaginator time to first index: {elapsed:.4f} seconds")
                        start = None
                    print(f"EfficientQueryPaginator page {count}, items: {len(pks)}")
                yield from self.value_query.filter(pk__in=pks)
                pks = []
        
        # after iteration, any remaining pks
        if pks:
            yield from self.value_query.filter(pk__in=pks)
    
    def paginate(self) -> Generator[list, None, None]:
        """ Grab a page of PKs, return results in bulk. (Use this one 99% of the time) """
        if self.verbose:
            print("EfficientQueryPaginator paginate start")
        pks = []
        
        start = perf_counter()
        for count, pk in enumerate(self.pk_query.iterator(chunk_size=self.page_size), start=1):
            pks.append(pk)
            if count % self.page_size == 0:
                if self.verbose:
                    if start:
                        elapsed = perf_counter() - start
                        print(f"EfficientQueryPaginator time to first index: {elapsed:.4f} seconds")
                        start = None
                    print(f"EfficientQueryPaginator page {count}, items: {len(pks)}")
                # using list(query) the iteration occurs inside cpython and is extremely quick.
                # Do not create a variable that references the list! that creates a reference!
                # (it might still have a reference, this is very hard to test.)
                yield list(self.value_query.filter(pk__in=pks))
                pks = []
        
        # after iteration, any remaining pks
        if pks:
            if self.verbose:
                print(f"EfficientQueryPaginator final page, items: {len(pks)}")
            yield list(self.value_query.filter(pk__in=pks))
    
    def stream_csv(self, header_names: list[str] = None) -> Generator[str, None, None]:
        """ Streams out a page by page csv file for passing into a FileResponse. """
        if not self.doing_values_list:
            raise Exception("stream_csv requires use of values_list parameter.")
        
        # StreamingStringsIO is might be less efficient than perfect StreamingBytesIO streaming,
        # but it handles some type conversion cases
        si = StreamingStringsIO()  # use our special streaming class to make this work
        filewriter = csv.writer(si)
        filewriter.writerow(self.values_list if header_names is None else header_names)
        # yield the header row
        yield si.getvalue()
        si.empty()
        
        # use the bulk writerows function, should be faster.
        rows: list[tuple] = []
        for rows in self.paginate():
            filewriter.writerows(rows)
            yield si.getvalue()
            si.empty()
    
    def stream_json_paginate(self, **kwargs):
        return self.stream_orjson_paginate(**kwargs)
    
    def stream_orjson_paginate(self, **kwargs) -> Generator[bytes, None, None]:
        """ Streams a page by page orjson'd bytes of json list elements. Accepts kwargs for orjson. """
        
        mutate = hasattr(self, "mutate_query_results")
        
        # We are going for maximum throughput with minimum memory usage. We reduce the load inside
        # the loop where there would need to be a check of which, and manage memory usage.
        # do the query before yielding the first page, this is the only way to get the first page
        paginator = self.paginate()   # 0x memory, just the iterator
        
        # usage of next raises a StopIteration exception if the iterator is empty.
        try:
            first_page = next(paginator)  # 1x memory
        except StopIteration:
            yield b"[]"
            return
        
        if mutate:
            self.mutate_query_results(first_page)
        
        # documented inside the loop
        out_raw = orjson_dumps(first_page, **kwargs)  # 2x memory
        del first_page                                # 1x memory
        out_final = out_raw[0:-1]                     # 2x memory
        del out_raw                                   # 1x memory
        yield out_final
        del out_final                                 # 0x memory
        
        # if we have a single page the iterator is empty and the body of the loop is skipped.
        for page in paginator:
            yield b","
            
            if mutate:
                self.mutate_query_results(page)
            
            # this is a bytes object, we cut the first and last characters (brackets) off
            # unfortunately this results in a copy. Fairly certain there is no way to solve the
            # overhead of the page variable because even if we del page there is a reference in the
            # paginator scope. However, if that is not the case, then with some stupid shuffling and
            # careful calls to del we can at least get to a point where the garbage collector
            # _might_ clean up out_raw and page before we yield out_final.
            out_raw = orjson_dumps(page, **kwargs)  # 2x memory
            del page                                # 1x memory
            out_final = out_raw[1:-1]               # 2x memory
            del out_raw                             # 1x memory
            yield out_final
            del out_final                           # 0x memory
        
        yield b"]"
