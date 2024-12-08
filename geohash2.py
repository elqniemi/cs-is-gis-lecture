import heapq
from math import radians, sin, cos, sqrt, atan2

class GeoHash:
    __base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    __base32_map = {c: i for i, c in enumerate(__base32)}
    __neighbors = {
        'n': ['p0r21436x8zb9dcf5h7kjnmqesgutwvy', 
              'bc01fg45238967deuvhjyznpkmstqrwx'],
        's': ['14365h7k9dcfesgujnmqp0r2twvyx8zb', 
              '238967debc01fg45kmstqrwxuvhjyznp'],
        'e': ['bc01fg45238967deuvhjyznpkmstqrwx', 
              'p0r21436x8zb9dcf5h7kjnmqesgutwvy'],
        'w': ['238967debc01fg45kmstqrwxuvhjyznp', 
              '14365h7k9dcfesgujnmqp0r2twvyx8zb'],
    }
    __borders = {
        'n': ['prxz', 'bcfguvyz'],
        's': ['028b', '0145hjnp'],
        'e': ['bcfguvyz', 'prxz'],
        'w': ['0145hjnp', '028b'],
    }

    @staticmethod
    def encode_geohash(lat, lon, precision=5):
        lat_interval = [-90.0, 90.0]
        lon_interval = [-180.0, 180.0]
        geohash = []
        bit = 0
        ch = 0
        even = True

        while len(geohash) < precision:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if lon > mid:
                    ch |= 1 << (4 - bit)
                    lon_interval[0] = mid
                else:
                    lon_interval[1] = mid
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if lat > mid:
                    ch |= 1 << (4 - bit)
                    lat_interval[0] = mid
                else:
                    lat_interval[1] = mid

            even = not even
            if bit < 4:
                bit += 1
            else:
                geohash += GeoHash.__base32[ch]
                bit = 0
                ch = 0

        return ''.join(geohash)

    @staticmethod
    def decode_geohash(gh):
        lat_interval = [-90.0, 90.0]
        lon_interval = [-180.0, 180.0]
        even = True

        for c in gh:
            cd = GeoHash.__base32_map[c]
            for mask in [16, 8, 4, 2, 1]:
                if even:
                    if cd & mask:
                        lon_interval[0] = (lon_interval[0] + lon_interval[1]) / 2
                    else:
                        lon_interval[1] = (lon_interval[0] + lon_interval[1]) / 2
                else:
                    if cd & mask:
                        lat_interval[0] = (lat_interval[0] + lat_interval[1]) / 2
                    else:
                        lat_interval[1] = (lat_interval[0] + lat_interval[1]) / 2
                even = not even

        lat = (lat_interval[0] + lat_interval[1]) / 2
        lon = (lon_interval[0] + lon_interval[1]) / 2
        return (lat, lon)

    @staticmethod
    def calculate_neighbors(gh):
        if not gh:
            return {}
        last_char = gh[-1]
        parent = gh[:-1]
        type = len(gh) % 2
        neighbors = {}
        for direction in ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw']:
            neighbors[direction] = GeoHash.__calculate_neighbor(gh, direction)
        return neighbors

    @staticmethod
    def __calculate_neighbor(gh, direction):
        if not gh:
            return ''
        last_char = gh[-1]
        parent = gh[:-1]
        type = len(gh) % 2

        base = GeoHash.__neighbors
        borders = GeoHash.__borders

        dir_main = direction[0]
        if len(direction) > 1:
            dir_second = direction[1]
        else:
            dir_second = None

        if last_char in GeoHash.__borders[dir_main][type]:
            parent = GeoHash.__calculate_neighbor(parent, dir_main)
        neighbor_base = parent

        index = GeoHash.__base32_map[last_char]
        neighbor_char = GeoHash.__base32[GeoHash.__base32_map[last_char]]
        if neighbor_base:
            neighbor_char = GeoHash.__base32[(GeoHash.__base32_map[last_char] + GeoHash.__base32_map[GeoHash.__neighbors[dir_main][type].find(last_char)] ) % 32]
        neighbor = neighbor_base + neighbor_char

        if dir_second:
            neighbor = GeoHash.__calculate_neighbor(neighbor, dir_second)

        return neighbor

    @staticmethod
    def calculate_neighbors(gh):
        directions = ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw']
        neighbors = {}
        for direction in directions:
            neighbors[direction] = GeoHash.neighbor(gh, direction)
        return neighbors

    @staticmethod
    def neighbor(gh, direction):
        if not gh:
            return ''

        last_char = gh[-1]
        parent = gh[:-1]
        type = len(gh) % 2

        base = GeoHash.__neighbors
        borders = GeoHash.__borders

        if direction in ['n', 's', 'e', 'w']:
            main_dir = direction
            second_dir = None
        elif direction in ['ne', 'nw', 'se', 'sw']:
            main_dir = direction[0]
            second_dir = direction[1]
        else:
            return ''

        if last_char in GeoHash.__borders[main_dir][type]:
            parent = GeoHash.neighbor(parent, main_dir)
        neighbor_base = parent

        neighbor_index = GeoHash.__base32_map[last_char]
        neighbor_char = GeoHash.__base32[(neighbor_index + 1) % 32] 

        neighbor = neighbor_base + neighbor_char

        if second_dir:
            neighbor = GeoHash.neighbor(neighbor, second_dir)

        return neighbor

    # perf things
    @staticmethod
    def geohash_to_int(geohash: str) -> int:
        base32 = GeoHash.__base32
        base32_map = {char: i for i, char in enumerate(base32)}
        
        result = 0
        for char in geohash:
            result = result * 32 + base32_map[char]
        return result

    @staticmethod
    def int_to_geohash(geohash_int: int, precision: int) -> str:
        base32 = GeoHash.__base32
        result = ""
        for _ in range(precision):
            result = base32[geohash_int & 31] + result
            geohash_int >>= 5
        return result

    # ex 1: geohash nn
    @staticmethod
    def calculate_neighbors(gh):
        directions = ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw']
        neighbors = {}
        for direction in directions:
            neighbors[direction] = GeoHash.neighbor(gh, direction)
        return neighbors

    # ex 2: prefix matching for proximity
    @staticmethod
    def check_proximity(geohash1: str, geohash2: str, prefix_length: int) -> bool:
        return geohash1.startswith(geohash2[:prefix_length])

    @staticmethod
    def quadtree_split(geohash: str, min_hash: str = None, max_hash: str = None) -> list:
        base32 = GeoHash.__base32
        children = [geohash + char for char in base32]
        
        # Filter children if min_hash and max_hash are provided
        if min_hash and max_hash:
            children = [gh for gh in children if min_hash <= gh <= max_hash]
        
        # Create a 2D matrix to represent the Z-order
        size = int(sqrt(len(children)))
        matrix = [children[i * size:(i + 1) * size] for i in range(size)]
        
        return matrix
    
    @staticmethod
    def quadtree_split_geo(geohash: str, min_hash: str = None, max_hash: str = None) -> list:
        if len(geohash) % 2 == 0:
            return GeoHash.quadtree_split_even(geohash, min_hash, max_hash)
        else:
            return GeoHash.quadtree_split_odd(geohash, min_hash, max_hash)

    @staticmethod
    def quadtree_split_even(geohash: str, min_hash: str = None, max_hash: str = None) -> list:
        # Z-order for even-length geohashes
        z_order_matrix = [
            ['b', 'c', 'f', 'g', 'u', 'v', 'y', 'z'],
            ['8', '9', 'd', 'e', 's', 't', 'w', 'x'],
            ['2', '3', '6', '7', 'k', 'm', 'q', 'r'],
            ['0', '1', '4', '5', 'h', 'j', 'n', 'p']
        ]
        
        children = {}
        for row in z_order_matrix:
            for cell in row:
                child = geohash + cell
                if min_hash and max_hash:
                    if min_hash <= child <= max_hash:
                        children[cell] = child
                else:
                    children[cell] = child
        
        return [[children.get(cell, '') for cell in row] for row in z_order_matrix]

    @staticmethod
    def quadtree_split_odd(geohash: str, min_hash: str = None, max_hash: str = None) -> list:
        # Z-order for odd-length geohashes
        z_order_matrix = [
            ['p', 'r', 'x', 'z'],
            ['n', 'q', 'w', 'y'],
            ['j', 'm', 't', 'v'],
            ['h', 'k', 's', 'u'],
            ['5', '7', 'e', 'g'],
            ['4', '6', 'd', 'f'],
            ['1', '3', '9', 'c'],
            ['0', '2', '8', 'b']
        ]
        
        children = {}
        for row in z_order_matrix:
            for cell in row:
                child = geohash + cell
                if min_hash and max_hash:
                    if min_hash <= child <= max_hash:
                        children[cell] = child
                else:
                    children[cell] = child
        
        return [[children.get(cell, '') for cell in row] for row in z_order_matrix]
    
    # ex 4: bitmasking for bbox
    @staticmethod
    def create_bitmask(precision: int) -> int:
        return (1 << (5 * precision)) - 1

    @staticmethod
    def check_within_bounds(geohash: str, min_hash: str, max_hash: str) -> bool:
        precision = min(len(geohash), len(min_hash), len(max_hash))
        mask = GeoHash.create_bitmask(precision)
        
        geohash_int = GeoHash.geohash_to_int(geohash)
        min_int = GeoHash.geohash_to_int(min_hash)
        max_int = GeoHash.geohash_to_int(max_hash)
        
        geohash_shift = 5 * (len(geohash) - precision)
        min_shift = 5 * (len(min_hash) - precision)
        max_shift = 5 * (len(max_hash) - precision)
        
        geohash_int = geohash_int >> geohash_shift
        min_int = min_int >> min_shift
        max_int = max_int >> max_shift
        
        geohash_int = geohash_int & mask
        min_int = min_int & mask
        max_int = max_int & mask
        
        return min_int <= geohash_int <= max_int
    
    @staticmethod
    def check_within_bounds_lexicographic(geohash: str, min_hash: str, max_hash: str) -> bool:
        precision = min(len(geohash), len(min_hash), len(max_hash))
        return min_hash[:precision] <= geohash[:precision] <= max_hash[:precision]

    # ex 5: z-order curve
    @staticmethod
    def z_order_range_query(geohash: str, lower_bound: str, upper_bound: str) -> bool:
        return lower_bound <= geohash <= upper_bound