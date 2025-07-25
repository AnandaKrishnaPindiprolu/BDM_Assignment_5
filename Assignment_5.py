import redis
import codecs
import csv
from traceback import print_exc

try:
    from redis.commands.search.field import TextField, NumericField, TagField
    from redis.commands.search.indexDefinition import IndexDefinition, IndexType
    from redis.commands.search.query import Query

    SEARCH_ENABLED = True
except ImportError:
    SEARCH_ENABLED = False

class RedisUtility:
    def __init__(self):
        self.connection = None

    def initialize(self):
        try:
            self.connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            if self.connection.ping():
                return True
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            print_exc()
        return False

    def import_user_file(self, filepath):
        try:
            with codecs.open(filepath, 'r', encoding='utf-8') as f:
                data_lines = f.read().splitlines()

            batch = self.connection.pipeline()
            count = 0

            for line in data_lines:
                tokens, buf, quoted = [], "", False
                for ch in line:
                    if ch == '"':
                        quoted = not quoted
                    elif ch == ' ' and not quoted:
                        if buf:
                            tokens.append(buf)
                            buf = ""
                    else:
                        buf += ch
                if buf:
                    tokens.append(buf)

                if len(tokens) >= 22:
                    uid = tokens[0]
                    profile = {
                        'first_name': tokens[2],
                        'last_name': tokens[4],
                        'email': tokens[6],
                        'gender': tokens[8],
                        'ip_address': tokens[10],
                        'country': tokens[12],
                        'country_code': tokens[14],
                        'city': tokens[16],
                        'longitude': tokens[18],
                        'latitude': tokens[20],
                        'last_login': tokens[22] if len(tokens) > 22 else tokens[21]
                    }
                    batch.hset(f"user:{uid}", mapping=profile)
                    count += 1

            batch.execute()
            return count
        except Exception:
            print_exc()
            return 0

    def import_scores(self):
        try:
            with codecs.open('userscores.csv', 'r', encoding='utf-8') as score_file:
                reader = csv.DictReader(score_file)
                pipeline = self.connection.pipeline()
                records = 0

                for row in reader:
                    key = f"board:{row['leaderboard']}"
                    score = float(row['score'])
                    user = row['user:id']
                    pipeline.zadd(key, {user: score})
                    records += 1

                pipeline.execute()
                return records
        except Exception:
            print_exc()
            return 0

    def setup_search(self):
        if not SEARCH_ENABLED:
            return False

        try:
            index_def = IndexDefinition(index_type=IndexType.HASH, prefix=["user:"])
            schema = [
                TextField("first_name"),
                TextField("last_name"),
                TextField("email"),
                TagField("gender"),
                TagField("country"),
                TagField("country_code"),
                TextField("city"),
                NumericField("latitude"),
                NumericField("longitude"),
                NumericField("last_login")
            ]
            self.connection.ft("users_idx").create_index(schema, definition=index_def)
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                return True
            print(f"Error creating index: {e}")
            return False

    def get_user_details(self, user_id):
        try:
            data = self.connection.hgetall(f"user:{user_id}")
            return data
        except Exception:
            return {}

    def get_user_location(self, user_id):
        try:
            loc = self.connection.hmget(f"user:{user_id}", 'longitude', 'latitude')
            coords = {'longitude': loc[0], 'latitude': loc[1]} if all(loc) else None
            return coords
        except Exception:
            return {}

    def find_even_id_users(self):
        try:
            results = []
            names = []
            cursor = 0
            while True:
                cursor, keys = self.connection.scan(cursor, match="user:*", count=50)
                for key in keys:
                    uid = key.split(":")[1]
                    if uid[0] in '02468':
                        lname = self.connection.hget(key, 'last_name')
                        if lname:
                            results.append(key)
                            names.append(lname)
                if cursor == 0:
                    break
            return results, names
        except Exception:
            return [], []

    def locate_female_users(self):
        try:
            found = []

            if SEARCH_ENABLED:
                self.setup_search()
                q = Query("@gender:{female} (@country:{China}|@country:{Russia}) @latitude:[40 46]")
                output = self.connection.ft("users_idx").search(q)
                for item in output.docs:
                    found.append({
                        'id': item.id,
                        'first_name': getattr(item, 'first_name', ''),
                        'last_name': getattr(item, 'last_name', ''),
                        'country': getattr(item, 'country', ''),
                        'latitude': getattr(item, 'latitude', ''),
                        'email': getattr(item, 'email', '')
                    })
            else:
                cursor = 0
                while True:
                    cursor, keys = self.connection.scan(cursor, match="user:*", count=50)
                    for key in keys:
                        udata = self.connection.hgetall(key)
                        try:
                            lat = float(udata.get('latitude', '0'))
                            if (
                                udata.get('gender') == 'female' and
                                udata.get('country') in ['China', 'Russia'] and
                                40 <= lat <= 46
                            ):
                                found.append({
                                    'id': key,
                                    'first_name': udata.get('first_name'),
                                    'last_name': udata.get('last_name'),
                                    'country': udata.get('country'),
                                    'latitude': udata.get('latitude'),
                                    'email': udata.get('email')
                                })
                        except ValueError:
                            continue
                    if cursor == 0:
                        break
            return found
        except Exception:
            return []

    def top_leaderboard_emails(self):
        try:
            ranked = self.connection.zrevrange("board:2", 0, 9, withscores=True)
            emails = []
            for uid, score in ranked:
                email = self.connection.hget(uid, 'email')
                if email:
                    emails.append(email)
            return emails
        except Exception:
            return []

if __name__ == "__main__":
    redis_tool = RedisUtility()
    if redis_tool.initialize():
        redis_tool.import_user_file("users.txt")
        redis_tool.import_scores()
        redis_tool.setup_search()
        redis_tool.get_user_details(1)
        redis_tool.get_user_location(2)
        redis_tool.find_even_id_users()
        redis_tool.locate_female_users()
        redis_tool.top_leaderboard_emails()
    else:
        print("Redis setup unsuccessful.")
