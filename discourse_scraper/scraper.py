import db_drivers
import requests
from ScrapeSession import ScrapeSession

POST_BY_REQUEST=30

def iterate_on_pages(sess, *args, **kwargs):
    if not "params" in kwargs:
        kwargs["params"] = {}

    for i in range(0, 100000):
        kwargs["params"]["page"] = i
        yield sess.get(*args, **kwargs).json(), i

def scrape(output, driver_name, url):
    print(f"scraping {url}...")
    sess = ScrapeSession()

    driver = db_drivers.drivers[driver_name](output, url)
    last_topic_id = driver.get_last_topic_id()

    driver.save_basic_info(sess.get(url+"/site/basic-info.json").json())
    driver.save_categories(sess.get(url+"/categories.json", params={"include_subcategories": "true"}).json()["category_list"]["categories"])

    for data, page_id in iterate_on_pages(sess, url+"/latest.json", params={"ascending": "false", "order": "created"}):
        has_reached_topic_id = False
        if len(data["topic_list"]["topics"]) == 0:
            print("[discovery] no more result")
            break

        for topic in data["topic_list"]["topics"]:
            if last_topic_id is not None and topic["id"] <= last_topic_id and has_reached_topic_id == False:
                print("[discovery] reached the last topic id, continuing this page then stop.")
                has_reached_topic_id = True
            driver.save_topic(topic, False)
        
        print(f"[discovery] page {page_id+1} done.")
        if has_reached_topic_id:
            break
    driver.flush()

    def retrieve_topic_posts(topic_id):
        print(f"[topic-{topic_id}] fetching topic data...")
        data = sess.get(url+f"/t/{topic_id}.json").json()
        driver.save_topic(data, True)

        last_topic_post_id = driver.get_topic_highest_post_number(topic_id)
        target_post_ids = [
            post_id for post_id in data["post_stream"]["stream"]
            if last_topic_post_id is None or post_id > last_topic_post_id]
        target_post_ids.sort()

        print(f"[topic-{topic_id}] {len(target_post_ids)} new post(s) to fetch...")

        for i in range(0, len(target_post_ids), POST_BY_REQUEST):
            chunk_post_ids = target_post_ids[i:i + POST_BY_REQUEST]
            print(f"[topic-{topic_id}] fetching posts from {chunk_post_ids[0]} to {chunk_post_ids[-1]}")
            posts = sess.get(url+f"/t/{topic_id}/posts.json",
                params={
                    "post_ids[]": chunk_post_ids
                }).json()
            driver.save_posts(posts["post_stream"]["posts"])
        driver.flush()

    print("[scraping] checking for new posts")
    for data, page_id in iterate_on_pages(sess, url+"/latest.json", params={"ascending": "false", "order": "activity"}):
        if len(data["topic_list"]["topics"]) == 0:
            print("[scraping] no more result")
            break

        for topic in data["topic_list"]["topics"]:
            highest_post_number = driver.get_topic_highest_post_number(topic["id"])
            if highest_post_number is None or highest_post_number < topic["highest_post_number"]:
                print(f"[scraping] topic {topic['id']} has new posts")
                retrieve_topic_posts(topic['id'])

        print(f"[scraping] page {page_id+1} done.")

    not_full_users = []
    print("[user-enumeration] enumerating users")
    for response, page_id in iterate_on_pages(sess, url+"/directory_items.json", params={
            "period": "all",
            "order": "likes_received"}):
        if len(response["directory_items"]) == 0:
            print("[user-enumeration] no more result")
            break

        for user in response["directory_items"]:
            user = user["user"]
            driver.save_user(user, False)
            if not driver.has_full_user(user["id"]):
                print(f"[user-enumeration] user {user['id']} has not been fully scraped, marked for future scrape.")
                not_full_users.append((user["id"], user["username"]))
        driver.flush()

        print(f"[user-enumeration] page {page_id+1} done.")

    print("[full-user-scraping] scrapping new users data...")
    for user in not_full_users:
        print(f"[full-user-scraping] fetching user {user[0]} ({user[1]})")
        response = sess.get(url+"/u/"+user[1]+".json").json()
        user_data = response["user"]
        del response["user"]
        user_data["__additional"] = response
        driver.save_user(user_data, True)

    driver.close()