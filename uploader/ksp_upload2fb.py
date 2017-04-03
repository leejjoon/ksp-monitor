import re
import pyrebase
import logging


def get_firebase_default():
    config = {
        "apiKey": "apiKey",
        "authDomain": "ksp-status.firebaseapp.com",
        "databaseURL": "https://ksp-status.firebaseio.com",
        "storageBucket": "ksp-status.appspot.com",
        "serviceAccount": "ksp-status-23896e5d95b1.json"
        }

    firebase = pyrebase.initialize_app(config)

    return firebase


class FailSafeUploader(object):
    def __init__(self, msg_queue, quit_event, get_firebase=None):
        if get_firebase is None:
            get_firebase = get_firebase_default

        self.get_firebase = get_firebase

        self.msg_queue = msg_queue
        self.quit_event = quit_event

        logging.warn("fb initialized")

    def push(self, db, parent, entry):
        basename = entry.get("basename", None)
        if basename is None:
            db.child(parent).push(entry)
        else:
            db.child(parent).child(basename.replace(".", ":")).set(entry)

    def upload(self, parent):
        last_msg = None
        do_loop = True
        while do_loop:
            if self.quit_event.is_set():
                break

            logging.warn("fb starting loop")
            firebase = self.get_firebase()
            db = firebase.database()

            try:
                while True:
                    if last_msg is not None:
                        msg = last_msg
                    else:
                        msg = self.msg_queue.get()

                    last_msg = msg.copy()

                    if self.quit_event.is_set() or (msg["_task"] == "quit"):
                        do_loop = False
                        break
                    elif msg["_task"] == "upload":
                        msg.pop("_task")
                        logging.warn("fb uploading: {}".format(msg))
                        self.push(db, parent, msg)
                        logging.warn("fb uploaded")

                    last_msg = None

            except:
                import traceback
                traceback.print_exc()

        logging.warn("fb out of loop")


def test():
    from threading import Thread
    from queue import Queue

    print("starting")
    queue = Queue()
    uploader = FailSafeUploader(queue)
    t = Thread(target=uploader.upload)

    t.start()

    import time
    print("sleeping")
    time.sleep(5)

    queue.put(dict(_task="quit"))
    print("quit")
    t.join()
    print("joined")


# test()
