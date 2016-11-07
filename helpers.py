from bisect import bisect_left
from collections import deque, OrderedDict
from itertools import product
from marshal import loads, dumps
from math import log, radians, sin, cos, atan2, sqrt
from multiprocessing import Lock
from numbers import Number
from urllib import urlencode
from urlparse import parse_qsl


class LockedIterator(object):

    def __init__(self, iterator, lock_past, chunk=0):
        self.__iter = (obj for obj in iterator)
        self.__seen = 0
        self.__past = lock_past
        self.__hold = chunk
        self.__lock = Lock()

    def __add__(self, other):
        self.__seen += other
        return self

    def __sub__(self, other):
        self.__seen -= min(other, self.__seen)
        if self.__seen <= (self.__past - self.__hold):
            self.unlock()
        return self

    def __len__(self):
        return max(self.__seen, 0)

    def __iter__(self):
        while True:
            try:
                yield self.__next__()
            except:
                break

    def __next__(self):
        if self.__seen > self.__past:
            self.__lock.acquire()
            return self.__next__()
        else:
            self.__seen += 1
            return next(self.__iter)

    def __repr__(self):
        return '<iterlock %d / %d %slocked>' % (
            self.__seen, self.__past, '' if self.is_locked() else 'un')

    def next(self):
        return self.__next__()

    def __internal_state(self, open=False, lock=False):
        """
          Helper method which returns the internal state of the lock:
            True, if the lock is currently locked
            False, if the lock is currently opened

          This method can also be used to change the internal lock state:
            Given open=True, the lock will be opened
            Given lock=True, the lock will be closed
            Given open=False and lock=False, the lock's state will be
              determined and returned, after restoring the lock's state.

          See below for a functional explanation.

          The function threading.Lock.acquire takes a single positional
            argument, which defaults to True. This boolean flag allows the user
            to switch between two methods of Lock acquisition:

          threading.Lock.acquire(True) --> Block until the lock is acquired.
          threading.Lock.acquire(False) --> Attempt to acquire lock.
            If the lock is *already* acquired, return False,
            Else, acquire the lock, and return True.

          As such, method logic proceeds as follows:
          Situation A:
            threading.Lock.acquire(False) returns False,
            The lock was in a locked state, and still is.
          Situation B:
            threading.Lock.acquire(False) returns True,
            The lock was in an opened state, and is now locked.
          Regardless of situation, the lock is now locked. If the keyword
            argument "open" was passed in as True, we open the lock, and report
            the internal state as opened (False).
          Likewise, if "lock" was passed as True: we perform no further
            modifications to the lock itself, and report the internal state as
            locked (True).
          Otherwise, if the lock was originally open, it is now locked. We
            release the lock, then return the boolean flag originally received
            from threading.Lock.acquire(False).
        """
        was_open = self.__lock.acquire(False)
        if open:
            self.__lock.release()
            return False
        elif lock:
            return True
        elif was_open:
            self.__lock.release()
        return was_open

    def is_locked(self):
        return self.__internal_state()

    def unlock(self):
        return self.__internal_state(open=True)


class CacheDictionary(object):
    CACHE_SIZE_MIN = 10000

    @staticmethod
    def gen_cache_key(params):
        return urlencode(sorted([(k.encode('utf-8'), v.encode('utf-8'))
                                 for k, v in params.items()]))

    @staticmethod
    def restore_cache_key(key):
        return OrderedDict([(k.decode('utf-8'), v.decode('utf-8'))
                            for k, v in parse_qsl(key)])

    def __init__(self, maxsize=CACHE_SIZE_MIN, weakref=False):
        if maxsize < 1:
            raise ValueError("Cannot instantiate a dictionary with a zero or "
                             "negative key count!")
        super(self.__class__, self).__init__()
        self.__cache = dict()
        self.__keys = deque(maxlen=maxsize)
        self.__ref = weakref
        self.__reg = dict()
        self.__size = maxsize
        self.__store = maxsize

    @property
    def raw(self):
        return self.__cache

    @property
    def maxsize(self):
        return self.__store

    def __contains__(self, key):
        return self.__reg.get(key, key) in self.__cache

    def __iter__(self):
        return self.__cache.__iter__()

    def quiet_get(self, key):
        val = self.__cache.__getitem__(self.__reg.get(key, key))
        if self.__ref:
            return val
        else:
            return loads(val)

    def __getitem__(self, key):
        val = self.quiet_get(key)
        self.__prioritize(key)
        return val

    def register_alternate(self, key, alt):
        self.__reg[alt] = key

    def __setitem__(self, key, val):
        if key in self.__cache:
            adj = 0
        else:
            adj = 1
        if self.__ref:
            self.__cache.__setitem__(key, val)
        else:
            self.__cache.__setitem__(key, dumps(val))
        self.__size -= adj
        if self.__size < 0:
            self.__delete_oldest()
        self.__prioritize(key)

    def __delitem__(self, key):
        self.__cache.__delitem__(key)
        self.__keys.remove(key)
        purge = []
        for alt in self.__reg:
            if self.__reg[alt] == key:
                purge.append(alt)
        for alt in purge:
            del self.__reg[alt]
        self.__size += 1

    def __prioritize(self, key):
        try:
            self.__keys.remove(key)
        except ValueError:
            pass
        self.__keys.append(key)

    def __delete_oldest(self):
        self.__delitem__(self.__keys[0])


class RedBlackNode(object):
    RED = 0
    BLACK = 1

    @property
    def key(self):
        return self.__key

    @property
    def left(self):
        return self.__left

    @left.setter
    def left(self, left):
        if left is None or isinstance(left, RedBlackNode):
            self.__left = left

    @property
    def parent(self):
        return self.__parent

    @parent.setter
    def parent(self, parent):
        if parent is None or isinstance(parent, RedBlackNode):
            self.__parent = parent

    @property
    def right(self):
        return self.__right

    @right.setter
    def right(self, right):
        if right is None or isinstance(right, RedBlackNode):
            self.__right = right

    @property
    def grandparent(self):
        if self.__parent:
            return self.__parent.parent
        else:
            return None

    @property
    def great_uncle(self):
        grandparent = self.grandparent
        if grandparent:
            if grandparent.left == self.parent:
                return grandparent.right
            else:
                return grandparent.left
        else:
            return None

    @property
    def color(self):
        return self.__color

    @color.setter
    def color(self, color):
        if isinstance(color, int) and self.RED <= color <= self.BLACK:
            self.__color = color

    @property
    def values(self):
        if self.__ordered is None:
            return tuple().__iter__()
        else:
            return self.__ordered.__iter__()

    @property
    def ordered(self):
        if self.__ordered:
            return self.__ordered
        else:
            return []

    @property
    def attributes(self):
        if self.__attributes:
            return self.__attributes
        else:
            return {}

    __slots__ = ('__color', '__key', '__left', '__parent', '__right',
                 '__ordered', '__attributes', 'RED', 'BLACK')

    def __init__(self, key, parent=None, *order_attributes, **named_attributes):
        self.__color = self.RED
        self.__key = key
        self.__left = None
        self.__parent = parent
        self.__right = None

        if order_attributes:
            self.__ordered = list(order_attributes)
        else:
            self.__ordered = None

        if named_attributes:
            self.__attributes = dict(named_attributes)
        else:
            self.__attributes = None

    def __getattr__(self, attr):
        if self.__attributes is not None:
            return self.__attributes[attr]
        raise AttributeError(
            "'RedBlackNode' object has no attribute '%s'" % attr)

    def __setattr__(self, attr, item):
        try:
            object.__setattr__(self, attr, item)
        except:
            if not self.__attributes:
                self.__attributes = {}
            self.__attributes[attr] = item

    def __getitem__(self, idx_or_key):
        if isinstance(idx_or_key, int):
            return self.ordered[idx_or_key]
        else:
            return self.attributes[idx_or_key]

    def __setitem__(self, idx_or_key, val):
        if isinstance(idx_or_key, int):
            if not self.__ordered:
                raise IndexError("list index out of range")
            self.__ordered[idx_or_key] = val
        else:
            if not self.__attributes:
                self.__attributes = {}
            self.__attributes[idx_or_key] = val

    def extend(self, iterator):
        if not self.__ordered:
            self.__ordered = []
        self.__ordered.extend(iterator)

    def update(self, mapping):
        if not self.__attributes:
            self.__attributes = {}
        self.__attributes.update(mapping)

    def __unicode__(self):
        return(u'<%d %s node(*(%s), **%s)>' % (
            self.__key, 'R' if self.__color == self.RED else 'B',
            u','.join(map(unicode, self.ordered)), repr(self.attributes)))

    def __repr__(self):
        return('<%d %s node(*(%s), **%s)>' % (
            self.__key, 'R' if self.__color == self.RED else 'B',
            ','.join(map(repr, self.ordered)), repr(self.attributes)))


class RedBlackTree(object):
    RED = RedBlackNode.RED
    BLACK = RedBlackNode.BLACK

    __slots__ = ('__root', '__size')

    @property
    def min(self):
        if self.__root:
            node = self.__root
            while node.left:
                node = node.left
            return node
        return None

    @property
    def max(self):
        if self.__root:
            node = self.__root
            while node.right:
                node = node.right
            return node
        return None

    def __init__(self):
        self.__root = None
        self.__size = 0

    def __len__(self):
        return self.__size

    def __repr__(self):
        return('<%s(%d) object at 0x%x>'
               % (self.__class__.__name__, len(self), id(self)))

    def __str__(self):
        current = None
        maxdigit = int(log(max(self.max.key, abs(self.min.key)), 10) + 1)
        maxheight = int(log(int(2 * log(len(self) + 1, 2)), 10) + 1)
        representation = ''
        for level, node in self.iter_breadth_first(self.__root, _level=0):
            if level != current:
                representation += ('\n%%0%dd ' % maxheight) % level
                current = level
            representation += ('%% %dd, ' % maxdigit) % node.key
        return representation

    def iter_sorted(self, reverse=False):
        pass

    @classmethod
    def __iter_breadth_first(cls, node, _level=None):
        if node:
            split_stack = [node]
            while split_stack:
                for _ in xrange(len(split_stack)):
                    node = split_stack.pop(0)
                    if isinstance(_level, int):
                        yield _level, node
                    else:
                        yield node
                    if node.left:
                        split_stack.append(node.left)
                    if node.right:
                        split_stack.append(node.right)
                if isinstance(_level, int):
                    _level += 1

    def iter_breadth_first(self, _level=None):
        return self.__iter_breadth_first(self.__root, _level)

    @classmethod
    def __iter_depth_first(cls, node, _level=None):
        if node:
            level = _level + 1 if isinstance(_level, int) else None
            for node_ in cls.__iter_depth_first(node.left, level):
                yield node_
            if isinstance(_level, int):
                yield _level, node
            else:
                yield node
            for node_ in cls.__iter_depth_first(node.right, level):
                yield node_

    def iter_depth_first(self, _level=None):
        return self.__iter_depth_first(self.__root, _level)

    def __iter__(self):
        return self.__iter_depth_first(self.__root)

    def insert(self, key, *order, **named):
        if self.__root:
            parent = self.__root
            while True:
                if parent.key == key:
                    if order:
                        parent.extend(order)
                    if named:
                        parent.update(named)
                    return

                if key < parent.key:
                    if parent.left:
                        parent = parent.left
                    else:
                        node = parent.left = RedBlackNode(
                            key, parent, *order, **named)
                        break
                else:
                    if parent.right:
                        parent = parent.right
                    else:
                        node = parent.right = RedBlackNode(
                            key, parent, *order, **named)
                        break
        else:
            parent = None
            node = self.__root = RedBlackNode(key, None, *order, **named)

        grandparent = node.grandparent
        great_uncle = node.great_uncle
        while True:
            if parent is None:
                node.color = self.BLACK
                break
            if parent.color == self.BLACK:
                break
            if great_uncle and great_uncle.color == self.RED:
                parent.color = great_uncle.color = self.BLACK
                grandparent.color = self.RED
                node = grandparent
                parent = node.parent
                grandparent = node.grandparent
                great_uncle = node.great_uncle
                continue

            if node == parent.right and parent == grandparent.left:
                grandparent.left = node
                node.parent = grandparent
                parent.right = node.left
                if node.left:
                    node.left.parent = parent
                node.left = parent
                parent.parent = node
                node = parent
                parent = node.parent
            elif node == parent.left and parent == grandparent.right:
                grandparent.right = node
                node.parent = grandparent
                parent.left = node.right
                if node.right:
                    node.right.parent = parent
                node.right = parent
                parent.parent = node
                node = parent
                parent = node.parent
            parent.color = self.BLACK
            grandparent.color = self.RED
            if node == parent.left:
                grandparent.left = parent.right
                if parent.right:
                    parent.right.parent = grandparent
                parent.right = grandparent
            else:
                grandparent.right = parent.left
                if parent.left:
                    parent.left.parent = grandparent
                parent.left = grandparent

            great_grandparent = grandparent.parent
            if great_grandparent:
                if grandparent == great_grandparent.left:
                    great_grandparent.left = parent
                else:
                    great_grandparent.right = parent
            else:
                self.__root = parent
                parent.color = self.BLACK
            parent.parent = great_grandparent
            grandparent.parent = parent
            break
        self.__size += 1

    def find_between(self, *keys):
        if self.__root:
            keys = filter(lambda val: isinstance(val, Number), keys)
            start = min(keys)
            until = max(keys)

            stack = [self.__root]
            while stack:
                node = stack.pop()
                if node.key >= start and node.key <= until:
                    yield node
                if node.right and node.key < until:
                    stack.append(node.right)
                if node.left and node.key > start:
                    stack.append(node.left)

    def find_single_nearest(self, key):
        if self.__root and isinstance(key, Number):
            best = None
            stack = [self.__root]

            while stack:
                node = stack.pop()
                if node.key == key:
                    return node
                elif not best or abs(node.key - key) < abs(best.key - key):
                    best = node

                if node.right and node.key < key:
                    stack.append(node.right)
                elif node.left and node.key > key:
                    stack.append(node.left)

            return best

    def find_nearest(self, key, limit=None):
        start = self.find_single_nearest(key)
        if start:
            nodes = [start]
            stack = [abs(start.key - key)]
        else:
            raise StopIteration
        visit = set()

        while stack:
            best = nodes.pop(0)
            del stack[0]
            yield best

            for neighbor in (best.left, best.right, best.parent,
                             best.grandparent, best.great_uncle):
                if neighbor:
                    nkey = abs(neighbor.key - key)
                    if nkey not in visit:
                        index = bisect_left(stack, nkey)
                        nodes.insert(index, neighbor)
                        stack.insert(index, nkey)
                        visit.add(nkey)

    def find_nearest_by_function(self, key_or_node, fxn=None):
        nodes = []
        dists = []
        total = 0

        if isinstance(key_or_node, RedBlackNode):
            best = key_or_node
        else:
            best = self.find_single_nearest(key_or_node)

        if not hasattr(fxn, '__call__'):
            del fxn

            def fxn(a, b):
                return abs(a.key - b.key)

        for node in self.__iter_depth_first(self.__root):
            dist = fxn(node, best)
            if dist > 0:
                index = bisect_left(dists, dist, lo=0, hi=total)
                dists.insert(index, dist)
                nodes.insert(index, node)
                total += 1

        for node in nodes:
            yield node


class GeoTree(object):

    @classmethod
    def spawn_test_tree(cls, res=1):
        tree = cls()
        res = 1.0 / float(res)
        LAT = int(180 * res)
        LON = int(90 * res)
        for lat, lon in product(range(-LAT, LAT + 1, 1),
                                range(-LON, LON + 1, 1)):
            tree.insert(lat / res, lon / res)
        return tree

    def __init__(self):
        self.__tree = RedBlackTree()

    def __len__(self):
        return len(self.__tree)

    @staticmethod
    def __z_curve(x_coord, y_coord):
        bit = 1
        MAX = max(x_coord, y_coord)
        res = 0

        while bit <= MAX:
            bit <<= 1
        bit >>= 1

        while bit:
            res <<= 1
            if x_coord & bit:
                res |= 1
            res <<= 1
            if y_coord & bit:
                res |= 1
            bit >>= 1

        return res

    @classmethod
    def geo_curve(cls, latitude, longitude):
        return cls.__z_curve(int(round((latitude + 90.0) * 100000)),
                             int(round((longitude + 180.0) * 100000)))

    def insert(self, latitude, longitude, *order, **named):
        named.update({"latitude": latitude, "longitude": longitude})
        curve = self.geo_curve(latitude, longitude)
        self.__tree.insert(curve, *order, **named)

    def find_exactly(self, latitude, longitude):
        curve = self.geo_curve(latitude, longitude)
        return self.__tree.find_between(curve)

    def find_between(self, min_coords, max_coords):
        min_curve = self.geo_curve(*min_coords)
        max_curve = self.geo_curve(*max_coords)

        min_lat = min(min_coords[0], max_coords[0])
        min_lon = min(min_coords[1], max_coords[1])
        max_lat = max(min_coords[0], max_coords[0])
        max_lon = max(min_coords[1], max_coords[1])

        for node in self.__tree.find_between(min_curve, max_curve):
            if (min_lat <= node.latitude and min_lon <= node.longitude and
                    max_lat >= node.latitude and max_lon >= node.longitude):
                yield node

    @staticmethod
    def haversine(latitude1, longitude1, latitude2, longitude2, radius=6371):
        diff_lat = radians(latitude2 - latitude1)
        diff_lon = radians(longitude2 - longitude1)

        sin_half_diff_lat = sin(diff_lat / 2)
        sin_half_diff_lon = sin(diff_lon / 2)

        A = (sin_half_diff_lat * sin_half_diff_lat + cos(radians(latitude1)) *
             sin_half_diff_lon * sin_half_diff_lon * cos(radians(latitude2)))
        return radius * 2 * atan2(sqrt(A), sqrt(1 - A))

    @classmethod
    def __haversine(cls, node1, node2):
        return cls.haversine(node1.latitude, node1.longitude,
                             node2.latitude, node2.longitude)

    def find_approximate_nearest(self, latitude, longitude):
        return self.__tree.find_nearest(self.geo_curve(latitude, longitude))

    def find_exact_nearest(self, latitude, longitude):
        return self.__tree.find_nearest_by_function(
            self.geo_curve(latitude, longitude), fxn=self.__haversine)
