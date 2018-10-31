#!/usr/bin/env python3

import json
import random
from datetime import datetime, timedelta

#########################
# Ad logs json generator 
#
# Help : python botgen.py -h
#
# Examples : 
# Write log for 1 bot, 1000 users, 100 requestes/sec, duration 300 seconds 
#
#   python botgen.py -b 1 -u 1000 -n 100 -d 300 -f data.json
#
# Notes : 
#  bots have ip 172.20.X.X and make a transition  ~ 1 in sec
#  users have ip 172.10.X.X and make a transition  ~ 4 in sec
#

# == generate content ids for bots and users
# content ids [1000 .. 1020]
bot_categories = [id for id in range(1000, 1020)]
# bot changes content twice as much as an user
# conten ids [1000, 1000 ..  1010, 1010]
user_categories = bot_categories[:int(len(bot_categories) / 2)] * 2


# these funtions return random content id for users, bots
def random_content_user(): return random.choice(user_categories)


def random_content_bot(): return random.choice(bot_categories)


# generate random action for users, bots
# bots clicks more often that users
def random_action_user(): return random.choice(["view"] * 9 + ["click"]) # 1 / 10


def random_action_bot(): return random.choice(['click', 'click', 'click', 'view'])  # probabilities click/view = 75/25


def user2ip(id): return "172.10.{}.{}".format(int(id / 255), id % 255)


def bot2ip(id): return "172.20.{}.{}".format(int(id / 255), id % 255)


def asits(dt): return int(dt.timestamp())


def asJson(entry): return {'time': asits(entry[0]), 'categoryId': entry[1], 'ip': entry[2], 'action': entry[3]}


def writeAsJson(entry, fd=None):
    entry = asJson(entry)
    if fd:
        json.dump(entry, fd)
    else:
        print(entry)


# Log generator for users & bots
def generate_log(args, start_time):
    BOT_TRANSITION_EVERY_SEC = 2

    t1, t2 = start_time, start_time + timedelta(seconds=args.duration)

    users = range(0, args.users)
    while t1 < t2:
        for uid in random.sample(users, args.freq):
            yield (t1, random_content_user(), user2ip(uid), random_action_user())

        if (int(t1.timestamp()) % BOT_TRANSITION_EVERY_SEC == 0):
            for bid in range(0, args.bots):
                yield (t1, random_content_bot(), bot2ip(bid), random_action_bot())

        t1 += timedelta(seconds=1)

    print("generated for period :", start_time, t2)


def do_generate(fd=None):
    first = True
    for entry in generate_log(args, datetime.now()):
        if not first and fd:
            fd.write("\n")
        else:
            first = False
        writeAsJson(entry, fd)
        fd.flush()


def main(args):
    print("started with parameters :", args)
    if args.file:
        with open(args.file, 'a') as fd:
            do_generate(fd)
    else:
        do_generate()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bots', type=int, default=1, help="number of bots")
    parser.add_argument('-u', '--users', type=int, default=1000, help="number of users")
    parser.add_argument('-d', '--duration', type=int, default=300, help="log duration in sec")
    parser.add_argument('-n', '--freq', type=int, default=100, help="number of user's requests in sec")
    parser.add_argument('-f', '--file', type=str, default=None, help="write to file")
    args = parser.parse_args()

    main(args)
