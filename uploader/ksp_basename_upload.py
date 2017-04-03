import os
import signal
import curio

from asyncwatch import watch, EVENTS

import logging

import threading
from ksp_upload2fb import FailSafeUploader


# def fb_upload(msg_queue, quit_event):
#     t = Thread(target=uploader.upload)

#     t.start()

#     return t


def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    ch = logging.FileHandler("ksp_file_history.log")
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - '
                                  '%(levelname)s - %(message)s')

    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger

logger = get_logger()


watchdir_queue = curio.Queue()
# files_created_queue = curio.Queue()

# to communicate with FB thread
msg_queue = curio.UniversalQueue()
quit_event = curio.UniversalEvent()

async def watcher_spawner():
    async with curio.TaskGroup() as tg:
        async for watchdir in watchdir_queue:
            await tg.spawn(watch_continously(watchdir))
            # tasks.append(t)


def create_msg(root, filename):
    _postfix = "_tan.nh.phot.cat"
    if not filename.endswith(_postfix):
        return None

    msg = dict(_task="upload",
               rootdir=root,
               basename=filename.replace(_postfix, ""))

    return msg


async def watch_continously(root):
    try:
        while True:
            try:
                async for event in watch(root, (EVENTS.CREATE,
                                                EVENTS.MOVED_TO)):
                    # EVENTS.CLOSE, EVENTS.MODIFY)):
                    if event.is_dir:
                        if event.tp & EVENTS.CREATE:
                            logger.info("DIR Created: {}".format(event.name))
                            watchdir = os.path.join(root, event.name)
                            await watchdir_queue.put(watchdir)
                    else:
                        # Events act like bit masks
                        if event.tp & EVENTS.CREATE:
                            logger.info("Created: {}".format(event.name))
                            msg = create_msg(root, event.name)
                            if msg is not None:
                                await msg_queue.put(msg)

                        elif event.tp & EVENTS.MOVED_TO:
                            logger.info("Moved: {}".format(event.name))
                            msg = create_msg(root, event.name)
                            if msg is not None:
                                await msg_queue.put(msg)

            except OSError as e:
                logger.warn("OSError: {}".format(e.args))
            except:
                raise

    except curio.CancelledError:
        logger.info('Stop watching: {}'.format(root))
        raise


# Entry point for curio
async def mainloop(watchroot, db_parent):

    # to communicate with fb uploader

    # set up the firebase uploader
    parent = db_parent
    uploader = FailSafeUploader(msg_queue, quit_event)
    uploader_thread = threading.Thread(target=uploader.upload,
                                       args=(parent,))
    uploader_thread.start()

    curio_watcher_spawner = await curio.spawn(watcher_spawner())

    dirs = [root for root, dirs, files in os.walk(watchroot)]
    for watchdir in dirs:
        logger.info('Start watching: {}'.format(watchdir))
        await watchdir_queue.put(watchdir)
        # await tg.spawn(watch_continously(watchdir))
        # tasks.append(t)

    goodbye = curio.SignalEvent(signal.SIGINT, signal.SIGTERM)
    await goodbye.wait()

    print("stopping fb uploader")

    await quit_event.set()
    await msg_queue.put(dict(_task="quit"))

    await curio.run_in_thread(uploader_thread.join)

    try:
        await curio_watcher_spawner.cancel()
        logger.info('Quit.')

    except:
        import traceback
        traceback.print_exc()


def main(watchroot, db_parent):
    curio.run(mainloop(watchroot, db_parent))


if __name__ == "__main__":
    watchroot = "./watched"
    db_parent = "PROCESSED_test"
    main(watchroot, db_parent)
