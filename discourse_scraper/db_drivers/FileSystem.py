import os
import os.path
import datetime
import json

class FileSystemDriver(object):
    def __init__(self, output, url):
        if url.endswith("/"):
            url = url[:-1]
        self.dirname = f"{output}/{url.replace('://','-').replace('/','-')}/"
        os.makedirs(self.dirname, exist_ok=True)
        os.makedirs(self.dirname+"/topics/", exist_ok=True)

        self.data = {"last_topic_id": None, "topics_highest_post_number": {}}
        try:
            with open(self.dirname+"/infos.json", "r") as fp:
                self.data = json.load(fp)
        except FileNotFoundError:
            pass

    def __get_topic_dir_by_id(self, topic_id: int) -> str:
        return self.dirname+f"/topics/{int(int(topic_id)/100)}/{topic_id}/"

    def get_last_topic_id(self) -> None|int:
        return self.data["last_topic_id"]

    def get_topic_highest_post_number(self, topic_id) -> None|int:
        return self.data["topics_highest_post_number"].get(str(topic_id), None)

    # save or update topics
    def save_topic(self, topic, is_full):
        assert("id" in topic)
        path = self.__get_topic_dir_by_id(topic["id"])
        os.makedirs(path, exist_ok=True)
        topic_str = json.dumps(topic)
        with open(path+f"/topic.{'full' if is_full else 'summary'}.json", "w") as fp:
            fp.write(topic_str)
        if self.data["last_topic_id"] is None or topic["id"] > self.data["last_topic_id"]:
            self.data["last_topic_id"] = topic["id"]

    def save_posts(self, posts):
        sorted_posts = {}
        for post in posts:
            assert("id" in post and "topic_id" in post)
            topic_id = str(post["topic_id"])
            post_group_id = str(int(post["id"]/100))
            post_group = sorted_posts.get((topic_id, post_group_id), {})
            post_group[str(post["id"])] = post
            sorted_posts[(topic_id, post_group_id)] = post_group
        
        for k, v in sorted_posts.items():
            path = self.__get_topic_dir_by_id(k[0])+"/posts/"
            os.makedirs(path, exist_ok=True)

            current_posts = {}
            try:
                with open(path+f"/{k[1]}.json") as fp:
                    current_posts = json.load(fp)
            except FileNotFoundError:
                pass

            new_posts_str = json.dumps({**current_posts, **v})
            with open(path+f"/{k[1]}.json", "w") as fp:
                fp.write(new_posts_str)

            for vv in current_posts.values():
                last_post_id = self.get_topic_highest_post_number(k[0])
                if last_post_id is None or last_post_id < vv["id"]:
                    self.data["topics_highest_post_number"][k[0]] = vv["id"]
            for vv in v.values():
                last_post_id = self.get_topic_highest_post_number(k[0])
                if last_post_id is None or last_post_id < vv["id"]:
                    self.data["topics_highest_post_number"][k[0]] = vv["id"]

    def __get_user_dir_by_id(self, user_id: int) -> str:
        return self.dirname+f"/users/{int(int(user_id)/100)}/"
    
    def save_user(self, user, is_full):
        assert("id" in user)
        
        dir_path = self.__get_user_dir_by_id(user["id"])
        user_str = json.dumps(user)

        os.makedirs(dir_path, exist_ok=True)
        
        with open(dir_path+f"{user['id']}.{'full' if is_full else 'summary'}.json", "w") as fp:
            fp.write(user_str)

    def has_full_user(self, user_id):
        return os.path.exists(self.__get_user_dir_by_id(user_id)+f"/{user_id}.full.json")

    def flush(self):
        infos_str = json.dumps(self.data)
        with open(self.dirname+"/infos.json", "w") as fp:
            fp.write(infos_str)

    def close(self):
        self.flush()