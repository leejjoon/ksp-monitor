import sys
import ksp_basename_upload

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("{} WATCHROOT DB_PARENT".format(sys.argv[0]))
        sys.exit()

    watchroot = sys.argv[1]
    db_parent = sys.argv[2]
    print("watching {} - {}".format(watchroot, db_parent))
    ksp_basename_upload.main(watchroot, db_parent)
