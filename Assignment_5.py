import redis
import codecs
import csv

class RedisHelper:
    def __init__(self):
        self.db = None

    def connect(self):
        try:
            self.db = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            return self.db.ping()
        except Exception as error:
            print(error)
            return False

    def load_users(self, filepath):
        try:
            with codecs.open(filepath, 'r', encoding='utf-8') as file:
                lines = file.readlines()

            pipeline = self.db.pipeline()
            for line in lines:
                tokens, buffer, quoted = [], "", False
                for ch in line:
                    if ch == '"':
                        quoted = not quoted
                    elif ch == ' ' and not quoted:
                        if buffer:
                            tokens.append(buffer)
                            buffer = ""
                    else:
                        buffer += ch
                if buffer:
                    tokens.append(buffer)

                if len(tokens) >= 22:
                    uid = tokens[0]
                    data = {
                        "first_name": tokens[2],
                        "last_name": tokens[4],
                        "email": tokens[6],
                        "gender": tokens[8],
                        "ip_address": tokens[10],
                        "country": tokens[12],
                        "country_code": tokens[14],
                        "city": tokens[16],
                        "longitude": tokens[18],
                        "latitude": tokens[20],
                        "last_login": tokens[22] if len(tokens) > 22 else tokens[21]
                    }
                    pipeline.hset(f"user:{uid}", mapping=data)
            pipeline.execute()
        except Exception as e:
            print(e)

    def load_scores(self, scorefile='userscores.csv'):
        try:
            with codecs.open(scorefile, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                pipe = self.db.pipeline()
                for row in reader:
                    board_key = f"board:{row['leaderboard']}"
                    score = float(row['score'])
                    user_id = row['user:id']
                    pipe.zadd(board_key, {user_id: score})
                pipe.execute()
        except Exception as e:
            print(e)

    def query1(self, usr):
        try:
            return self.db.hgetall(f"user:{usr}")
        except Exception:
            return {}

    def query2(self, usr):
        try:
            values = self.db.hmget(f"user:{usr}", "longitude", "latitude")
            if all(values):
                return {"longitude": values[0], "latitude": values[1]}
            return {}
        except Exception:
            return {}

    def query3(self):
        try:
            result_keys = []
            result_names = []
            cursor = 0
            while True:
                cursor, keys = self.db.scan(cursor, match="user:*", count=50)
                for key in keys:
                    uid = key.split(":")[1]
                    if uid[0] not in "13579":
                        lname = self.db.hget(key, "last_name")
                        if lname:
                            result_keys.append(key)
                            result_names.append(lname)
                if cursor == 0:
                    break
            return result_keys, result_names
        except Exception:
            return [], []

    def query4(self):
        try:
            matched = []
            cursor = 0
            while True:
                cursor, keys = self.db.scan(cursor, match="user:*", count=50)
                for key in keys:
                    data = self.db.hgetall(key)
                    try:
                        lat = float(data.get("latitude", "0"))
                        if (
                            data.get("gender") == "female" and
                            data.get("country") in ["China", "Russia"] and
                            40 <= lat <= 46
                        ):
                            matched.append({
                                "id": key,
                                "first_name": data.get("first_name", ""),
                                "last_name": data.get("last_name", ""),
                                "country": data.get("country", ""),
                                "latitude": data.get("latitude", ""),
                                "email": data.get("email", "")
                            })
                    except ValueError:
                        continue
                if cursor == 0:
                    break
            return matched
        except Exception:
            return []

    def query5(self):
        try:
            top_users = self.db.zrevrange("board:2", 0, 9)
            emails = []
            for uid in top_users:
                email = self.db.hget(f"user:{uid}", "email")
                if email:
                    emails.append(email)
            return emails
        except Exception:
            return []

if __name__ == "__main__":
    redis_tool = RedisHelper()
    if redis_tool.connect():
        redis_tool.load_users("users.txt")
        redis_tool.load_scores()
        print(redis_tool.query1("3"))
        print(redis_tool.query2("3"))
        print(redis_tool.query3())
        print(redis_tool.query4())
        print(redis_tool.query5())
    else:
        print("Redis connection failed.")
