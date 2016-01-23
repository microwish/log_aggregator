#include "PyKafkaClient.h"
#include <string>
#include <vector>
#include <map>
#include <deque>
#include <stdio.h>
#include <stdlib.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include <dirent.h>
#include <stddef.h>
#include <errno.h>
#include <syslog.h>
#include <sys/inotify.h>
#include <time.h>
#include <poll.h>
#include <fcntl.h>
#include <signal.h>
#include <execinfo.h>
#include <sys/select.h>

#if 0
// base name and offset of a file
class FileOffset {
public:
    std::string filename; // basename
    long offset;
    FileOffset(): offset(0) {}
    FileOffset(const FileOffset& fo)
    {
        filename.assign(fo.filename);
        offset = fo.offset;
    }
    ~FileOffset() {}
    FileOffset& operator=(const FileOffset& rhs)
    {
        if (this != &rhs) {
            filename.assign(rhs.filename);
            offset = rhs.offset;
        }
        return *this;
    }
};
#endif

#define BASE_NAME_LEN 64

class FileOffset {
public:
    char filename[BASE_NAME_LEN]; // basename
    long offset;
    FileOffset(): offset(0) {}
    FileOffset(const FileOffset& fo)
    {
        strcpy(filename, fo.filename);
        offset = fo.offset;
    }
    ~FileOffset() {}
    FileOffset& operator=(const FileOffset& rhs)
    {
        if (this != &rhs) {
            strcpy(filename, rhs.filename);
            offset = rhs.offset;
        }
        return *this;
    }
};

#if 0
// /data/ef-logs/bid/20151009/0/201510091507.AM.0.bid.log
class PathComp {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const
    {
        const char *s1, *s2, *p1, *p2;
        int r;
        s1 = lhs.c_str() + LOG_PATH_ROOT_LEN;
        s2 = rhs.c_str() + LOG_PATH_ROOT_LEN;
        p1 = strchr(s1, '/');
        p2 = strchr(s2, '/');
        r = strncmp(p1 + 1, p2 + 1, 8);
        if (r == 0) {
            return strcmp(s1, s2) < 0;
        } else {
            return r < 0;
        }
    }
};
#endif

#define CWD_ROOT "/data/users/data-infra/log-aggregator"

// map of topic <--> raw path
static std::map<std::string, std::string> raw_paths;
// map of topic <--> normal path
static std::multimap<std::string, std::string> normal_paths;

static std::string today_ymd, yesterday_ymd;
static time_t zero_ts;

static std::deque<std::string> conveyor;
static pthread_mutex_t conveyor_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t conveyor_cond = PTHREAD_COND_INITIALIZER;

// map of inotify watch descriptor <--> inotify path
static std::map<int, std::string> wd_path_map;
// map of inotify watch descriptor <--> topic
static std::map<int, std::string> wd_topic_map;
static pthread_rwlock_t watch_lock = PTHREAD_RWLOCK_INITIALIZER;

static char *offset_archive_path = NULL;
// map of monitored path <--> metadata of current file
//static std::map<std::string, FileOffset, PathComp> path_offset_table;
static std::map<std::string, FileOffset> path_offset_table;
static pthread_rwlock_t offset_lock = PTHREAD_RWLOCK_INITIALIZER;

static char *brokers, *producer_conf;
static kafka_producer_t *producer;
static std::map<std::string, kafka_client_topic_t *> topics;

static char *app_log_path = NULL;

// retrieve the configured directories to be monitored
static void parse_conf_file(const char *conf_path)
{
    if (conf_path == NULL || conf_path[0] == '\0') {
        write_log(app_log_path, LOG_ERR, "bad conf path");
        return;
    }

    FILE *fp = fopen(conf_path, "r");
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "fopen[%s] failed with errno[%d]", conf_path, errno);
        return;
    }

    char buf[256];

    while (fgets(buf, sizeof(buf), fp) != NULL) {
        if (buf[0] == '\0') continue;

        char *p = buf;
        while (isspace(*p)) p++;
        if (*p == '#' || *p == '\0') continue;

        char *q = p + strlen(p) - 1;
        while (isspace(*q)) q--;
        if (q < p) {
            write_log(app_log_path, LOG_WARNING, "bad config line[%s]", buf);
            continue;
        }
        if (*q == '\n') *q = '\0'; else *++q = '\0';

        if ((q = strchr(p, ':')) == NULL) {
            write_log(app_log_path, LOG_WARNING,
                      "bad config line[%s] without a topic", buf);
            continue;
        }
        *q = '\0';
        raw_paths[p] = q + 1;
    }

    if (ferror(fp) != 0) {
        write_log(app_log_path, LOG_ERR, "fgets[%s] failed", conf_path);
        raw_paths.clear();
    }

    fclose(fp);
}

static std::string ymd_stringify(time_t t)
{
    std::string ymd;
    struct tm tm;
    if (localtime_r(&t, &tm) == NULL) {
        write_log(app_log_path, LOG_ERR, "localtime_r failed");
        return ymd;
    }
    char s[9];
    snprintf(s, sizeof(s), "%d%02d%02d",
             1900 + tm.tm_year, tm.tm_mon + 1, tm.tm_mday);
    ymd.assign(s);
    return ymd;
}

static inline bool dir_exists(const char *path)
{
    struct stat st;

    if (stat(path, &st) == 0) {
        return S_ISDIR(st.st_mode);
    } else {
        // XXX ENOENT
        return false;
    }
}

#define LOG_PATH_ROOT "/data/ef-logs/"
#define LOG_PATH_ROOT_LEN 14
#define YMD_HOLDER "[Ymd]"
#define YMD_HOLDER_LEN 5

// /data/ef-logs/unbid/[Ymd]/{8:0,1,2,3,4,5,6,7}:
// /data/ef-logs/unbid/20150821/0/ /data/ef-logs/unbid/20150821/7/
// /data/ef-logs/cvt/[Ymd]:
// /data/ef-logs/cvt/20150822/
//
// all normalized paths end with a "/"
static void normalize(const std::string& topic, const std::string& path,
                      std::vector<std::string>& result)
{
    // for lately generated date subdirs
    result.push_back(LOG_PATH_ROOT + topic + '/');

    char temp[96];
    int l = snprintf(temp, sizeof(temp), "%s%s/%s/",
                     LOG_PATH_ROOT, topic.c_str(), today_ymd.c_str());

    // for lately generated hashing subdirs
    result.push_back(temp);

    const char *p = strrchr(path.c_str(), '{');
    if (p != NULL) {
        int n = atoi(p + 1);
        const char *p2 = strchr(p, ':');
        if (p2 == NULL) {
            for (int i = 0; i < n; i++) {
                sprintf(temp + l, "%d/", i);
                result.push_back(temp);
            }
        } else {
            int m = -1;
            const char *p3 = strchr(++p2, ',');
            while (p3 != NULL) {
                m = atoi(p2);
                if (m < n) {
                    sprintf(temp + l, "%d/", atoi(p2));
                    result.push_back(temp);
                } else {
                    write_log(app_log_path, LOG_WARNING,
                              "invalid hashing dir[%d:%d] for topic[%s]",
                              n, m, topic.c_str());
                }
                p2 = p3 + 1;
                p3 = strchr(p2, ',');
            }
            m = atoi(p2);
            if (m < n) {
                sprintf(temp + l, "%d/", m);
                result.push_back(temp);
            } else {
                write_log(app_log_path, LOG_WARNING,
                          "invalid hashing dir[%d:%d] for topic[%s]",
                          n, m, topic.c_str());
            }
        }
    }
}

// put all directories for today in the map
// regardless of whether a dir exists now
static void normalize_paths()
{
    std::vector<std::string> temp;
    std::map<std::string, std::string>::const_iterator it = raw_paths.begin();
    for (; it != raw_paths.end(); it++) {
        temp.clear();
        normalize(it->first, it->second, temp);
        for (size_t i = 0; i < temp.size(); i++) {
            normal_paths.insert(std::pair<std::string, std::string>(it->first,
                                                                    temp[i]));
        }
    }
}

static bool is_normal_path(const std::string& topic, const std::string& path)
{
    std::pair<std::multimap<std::string, std::string>::iterator,
        std::multimap<std::string, std::string>::iterator> ret =
            normal_paths.equal_range(topic);
    for (std::multimap<std::string, std::string>::iterator it = ret.first;
         it != ret.second; it++) {
        if (it->second.compare(path) == 0) {
            return true;
        }
    }
    return false;
}

static inline int set_nonblocking(int fd)
{
    int old = fcntl(fd, F_GETFL);
    if (old == -1) {
        write_log(app_log_path, LOG_ERR,
                  "fcntl F_GETFL failed with errno[%d]", errno);
        return -1;
    }
    if (fcntl(fd, F_SETFL, old | O_NONBLOCK) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "fcntl F_SETFL failed with errno[%d]", errno);
        return -1;
    }
    return 0;
}

static int preprocess_inotify()
{
    int inot_fd = inotify_init();
    if (inot_fd == -1) {
        write_log(app_log_path, LOG_ERR,
                  "inotify_init failed with errno[%d]", errno);
        return -1;
    }

    if (set_nonblocking(inot_fd) != 0) {
        close(inot_fd);
        return -1;
    }

    for (std::multimap<std::string, std::string>::const_iterator it =
         normal_paths.begin(); it != normal_paths.end(); it++) {
        int wd = inotify_add_watch(inot_fd, it->second.c_str(),
                                   IN_MOVED_TO | IN_CREATE | IN_DONT_FOLLOW);
        if (wd == -1) {
            int errno_sv = errno;
            if (errno_sv != ENOENT) {
                write_log(app_log_path, LOG_ERR, "inotify_add_watch[%s] failed"
                          " with errno[%d]", it->second.c_str(), errno_sv);
                close(inot_fd);
                return -1;
            }
            write_log(app_log_path, LOG_INFO, "inotify_add_watch[%s] failed"
                      " with errno[%d]", it->second.c_str(), errno_sv);
        } else {
            wd_path_map[wd] = it->second;
            wd_topic_map[wd] = it->first;
        }
    }

    if (wd_path_map.size() == 0) {
        close(inot_fd);
        return -1;
    }

    return inot_fd;
}

static void destroy_topics()
{
    for (std::map<std::string, kafka_client_topic_t *>::iterator it =
         topics.begin(); it != topics.end(); it++) {
        del_topic(it->second);
    }
    topics.clear();
}

static bool init_topics()
{
    for (std::map<std::string, std::string>::const_iterator it =
         raw_paths.begin(); it != raw_paths.end(); it++) {
        kafka_client_topic_t *kct = set_producer_topic(producer,
                                                       it->first.c_str());
        if (kct == NULL) {
            destroy_topics();
            return false;
        }
        topics[it->first] = kct;
    }
    return true;
}

#define BATCH_NUM 100
#define BATCH_NUM_UNBID 300

static int produce_msgs_and_save_offset(kafka_client_topic_t *kct,
                                        char *fullpath,
                                        long offset = 0)
{
    if (kct == NULL) {
        write_log(app_log_path, LOG_ERR, "null KCT");
        return -1;
    }

    const char *topic = get_topic(kct);
    if (topic == NULL) {
        write_log(app_log_path, LOG_ERR, "get_topic[%s] failed", fullpath);
        return -1;
    }

    FILE *fp = fopen(fullpath, "r");
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR, "fopen[%s] for producing messages"
                  " failed with errno[%d]", fullpath, errno);
        return -1;
    }
    if (offset != 0) {
        if (fseek(fp, offset, SEEK_SET) != 0) {
            write_log(app_log_path, LOG_ERR, "fseek[%s] failed with errno[%d]",
                      fullpath, errno);
            fclose(fp);
            return -1;
        }
    }

    int num = 0, batch = BATCH_NUM;
    char buf[65536], *p = strrchr(fullpath, '/');
    std::vector<std::string> payloads, keys;
    FileOffset fo;

    strcpy(fo.filename, p + 1);
    *p = '\0';

    if (strcmp(topic, "unbid") == 0) batch = BATCH_NUM_UNBID;

    while (fgets(buf, sizeof(buf), fp) != NULL) {
        char *p2 = strrchr(buf, '\n');
        if (p2 != NULL) *p2 = '\0';
        if (buf[0] == '\0') {
            write_log(app_log_path, LOG_WARNING,
                      "empty log line in dir[%s] file[%s] line[%d]",
                      fullpath, fo.filename, num);
            continue;
        }
        payloads.push_back(buf);
        if (++num % batch == 0) {
            if (produce_messages(producer, kct, payloads, keys) <= 0) {
                write_log(app_log_path, LOG_ERR,
                          "produce_messages for topic[%s] failed", topic);
            } else {
                payloads.clear();
                fo.offset = ftell(fp);
#if 0
                try {
                    pthread_rwlock_rdlock(&offset_lock);
                    path_offset_table[fullpath] = fo;
                    pthread_rwlock_unlock(&offset_lock);
                } catch (std::exception& e) {
                    write_log(app_log_path, LOG_ERR, "saving offset in mem"
                              " failed with exception[%s]", e.what());
                } catch (...) {
                    write_log(app_log_path, LOG_ERR, "saving offset in mem"
                              " failed with unknown exception");
                }
#endif
                pthread_rwlock_rdlock(&offset_lock);
                std::map<std::string, FileOffset>::iterator it =
                    path_offset_table.find(fullpath);
                if (it == path_offset_table.end()) {
                    pthread_rwlock_unlock(&offset_lock);
                    pthread_rwlock_wrlock(&offset_lock);
                    path_offset_table[fullpath] = fo;
                } else {
                    it->second = fo;
                }
                pthread_rwlock_unlock(&offset_lock);
            }
        }
    }

    if (num % batch != 0) {
        if (produce_messages(producer, kct, payloads, keys) <= 0) {
            write_log(app_log_path, LOG_ERR,
                      "produce_messages for topic[%s] failed", topic);
        } else {
            fo.offset = ftell(fp);
            pthread_rwlock_rdlock(&offset_lock);
            std::map<std::string, FileOffset>::iterator it =
                path_offset_table.find(fullpath);
            if (it == path_offset_table.end()) {
                pthread_rwlock_unlock(&offset_lock);
                pthread_rwlock_wrlock(&offset_lock);
                path_offset_table[fullpath] = fo;
            } else {
                it->second = fo;
            }
            pthread_rwlock_unlock(&offset_lock);
        }
    }

    fclose(fp);

    *p = '/';
    write_log(app_log_path, LOG_INFO, "log sent [%s][%d]", fullpath, num);

    return num;
}

static bool extract_topic_from_path(const std::string& path, std::string& topic)
{
    if (path.length() <= LOG_PATH_ROOT_LEN) {
        write_log(app_log_path, LOG_WARNING,
                  "invalid path[%s] for extracting topic", path.c_str());
        return false;
    }

    const char *s = path.c_str() + LOG_PATH_ROOT_LEN, *p = strchr(s, '/');
    if (p == NULL) {
        write_log(app_log_path, LOG_WARNING,
                  "invalid path[%s] for extracting topic", path.c_str());
        return false;
    }

    topic.assign(s, p - s);
    return true;
}

#define POP_NUM 4

static void *foo(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread foo created");

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR,
                  "pthread_detach[foo] failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread foo exiting");
        return (void *)-1;
    }

    std::string paths[POP_NUM], topic;
    int n;

    do {
        if ((ret = pthread_mutex_lock(&conveyor_mtx)) != 0) {
            write_log(app_log_path, LOG_WARNING,
                      "pthread_mutex_lock[conveyor_mtx] failed with errno[%d]",
                      ret);
            sleep(1);
            continue;
        }
        while (conveyor.empty()) {
            write_log(app_log_path, LOG_INFO, "conveyor empty");
            pthread_cond_wait(&conveyor_cond, &conveyor_mtx);
        }
        for (n = 0; n < POP_NUM; n++) {
            paths[n].assign(conveyor.front());
            conveyor.pop_front();
            if (conveyor.empty()) {
                n++;
                break;
            }
        }
        if ((ret = pthread_mutex_unlock(&conveyor_mtx)) != 0) {
            write_log(app_log_path, LOG_WARNING,
                      "pthread_mutex_unlock[conveyor_mtx] failed"
                      " with errno[%d]", ret);
        }
        for (int i = 0; i < n; i++) {
            if (!extract_topic_from_path(paths[i], topic)) continue;
            produce_msgs_and_save_offset(topics[topic],
                                      const_cast<char *>(paths[i].c_str()));
        }
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread foo exiting");

    return (void *)0;
}

static void delay_simply(int milli)
{
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = milli * 1000;
    if (select(0, NULL, NULL, NULL, &tv) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "select failed with errno[%d]", errno);
    }
}

static void *handle_conveyor_backlog(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread handle_conveyor_backlog created");

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR,
                  "pthread_detach[handle_conveyor_backlog]"
                  " failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR,
                  "thread handle_conveyor_backlog exiting");
        return (void *)-1;
    }

    static int producer_thread_total = 1;

    do {
        sleep(120);

        // XXX
        if (conveyor.size() < 60) continue;
        pthread_t thr;
        if ((ret = pthread_create(&thr, NULL, foo, NULL)) != 0) {
            write_log(app_log_path, LOG_ERR, "pthread_create[foo] for speedup"
                      " failed with errno[%d]", ret);
            continue;
        }
        producer_thread_total++;
        write_log(app_log_path, LOG_INFO, "creating one more thread[%d] foo"
                  " for speedup", producer_thread_total);
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread handle_conveyor_backlog exiting");

    return (void *)0;
}

#define SCAN_DIRENT_MAX 8

static int scan_new_inotify_dir(const char *dir,
                                std::vector<std::string>& missing,
                                std::vector<unsigned char>& types)
{
    DIR *dp = opendir(dir);
    if (dp == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "opendir[%s] failed with errno[%d]",
                  dir, errno);
        return -1;
    }

    size_t el = offsetof(struct dirent, d_name) + 64;
    struct dirent *dep = (struct dirent *)malloc(el);
    if (dep == NULL) {
        write_log(app_log_path, LOG_ERR, "malloc failed");
        closedir(dp);
        return -1;
    }

    int n = 0, count = 0;
    struct dirent *res;
    while (readdir_r(dp, dep, &res) == 0) {
        if (res == NULL || n == SCAN_DIRENT_MAX) {
            ++count;
            if (count < 2 && n < SCAN_DIRENT_MAX) {
                // XXX once again to avoid losing entries
                n = 0;
                missing.clear();
                types.clear();
                delay_simply(800);
                rewinddir(dp);
                continue;
            } else {
                write_log(app_log_path, LOG_INFO,
                          "DEBUG %s leaving[%s] with entries[%d]",
                          __FUNCTION__, dir, n);
                free(dep);
                closedir(dp);
                return n;
            }
        }
        if (strcmp(dep->d_name, ".") == 0 || strcmp(dep->d_name, "..") == 0) {
            continue;
        }
        missing.push_back(dep->d_name);
        types.push_back(dep->d_type);
        n++;
    }

    write_log(app_log_path, LOG_ERR,
              "readdir_r[%s] failed after entries[%d] with errno[%d]",
              dir, n, errno);
    free(dep);
    closedir(dp);
    return -1;
}

// dir ends with a "/"
static int handle_new_inotify_dir(int inot_fd, const std::string& dir,
                                  const std::string& topic)
{
    std::vector<std::string> missing;
    std::vector<unsigned char> types;
    int n = scan_new_inotify_dir(dir.c_str(), missing, types);
    if (n < 0) {
        return 0;
    } else {
        std::vector<std::string> subdirs;
        int num = 0;
        if (n == 0) {
            write_log(app_log_path, LOG_INFO,
                      "seems no missing inotify events for dir[%s]",
                      dir.c_str());
        } else {
            write_log(app_log_path, LOG_INFO,
                      "inotify missing files[%s:%d]", dir.c_str(), n);
            std::string p;
            for (int i = 0; i < n; i++) {
                switch (types[i]) {
                case DT_REG:
                    p = dir + missing[i];
                    conveyor.push_back(p);
                    num++;
                    write_log(app_log_path, LOG_INFO,
                              "add missing file[%s]", p.c_str());
                    break;
                case DT_DIR:
                    p = dir + missing[i] + "/";
                    if (!is_normal_path(topic, p)) {
                        write_log(app_log_path, LOG_INFO,
                                  "DEBUG unexpected path[%s] from scan",
                                  p.c_str());
                        continue;
                    }
                    subdirs.push_back(p);
                    break;
                }
            }
        }
        // do it here to avoid repeated enqueuing
        int wd = inotify_add_watch(inot_fd, dir.c_str(),
                                   IN_MOVED_TO | IN_CREATE | IN_DONT_FOLLOW);
        if (wd == -1) {
            int errno_sv = errno;
            if (errno_sv == ENOENT) {
                write_log(app_log_path, LOG_INFO,
                          "inotify_add_watch[%s] failed"
                          " with errno[%d] at midday",
                          dir.c_str(), errno_sv);
            } else {
                write_log(app_log_path, LOG_ERR,
                          "inotify_add_watch[%s] failed"
                          " with errno[%d] at midday",
                          dir.c_str(), errno_sv);
            }
        } else {
            write_log(app_log_path, LOG_INFO,
                      "path[%s] is newly watched at midday", dir.c_str());
            wd_path_map[wd] = dir;
            wd_topic_map[wd] = topic;
        }

        for (size_t i = 0, l = subdirs.size(); i < l; i++) {
            num += handle_new_inotify_dir(inot_fd, subdirs[i], topic);
        }

        return num;
    }
}

#define INOT_BUF_LEN 8192

static int baz(int inot_fd)
{
    char buf[INOT_BUF_LEN]
        __attribute__ ((aligned(__alignof__(struct inotify_event))));
    struct inotify_event *evp;
    int num = 0;

    do {
        int n = read(inot_fd, buf, sizeof(buf));
        if (n == -1) {
            if (errno == EAGAIN) {
                write_log(app_log_path, LOG_INFO,
                          "reading inotify FD would block");
            } else {
                write_log(app_log_path, LOG_ERR,
                          "reading inotify FD failed with errno[%d]", errno);
                num = -1;
            }
            break;
        } else if (n == 0) {
            write_log(app_log_path, LOG_INFO, "no inotify events read");
            break;
        } else if (n == INOT_BUF_LEN) {
            write_log(app_log_path, LOG_WARNING, "inotify buf might overflow");
        }

        std::string path;

        for (char *p = buf; p < buf + n;
             p += sizeof(struct inotify_event) + evp->len) {
            evp = (struct inotify_event *)p;
            if ((evp->mask & IN_ISDIR) != 0) {
                if ((evp->mask & IN_CREATE) != 0) {
                    pthread_rwlock_rdlock(&watch_lock);
                    std::string& topic = wd_topic_map[evp->wd];
                    path.assign(wd_path_map[evp->wd]);
                    path.append(evp->name);
                    path.append("/");
                    if (is_normal_path(topic, path)) {
                        num += handle_new_inotify_dir(inot_fd, path, topic);
                    } else {
                        write_log(app_log_path, LOG_INFO,
                                  "DEBUG unexpected path[%s] from inotify",
                                  path.c_str());
                    }
                    pthread_rwlock_unlock(&watch_lock);
                } else {
                    write_log(app_log_path, LOG_WARNING,
                              "unexpected inotify dir event[%u]", evp->mask);
                }
            } else {
                if ((evp->mask & IN_MOVED_TO) != 0) {
                    pthread_rwlock_rdlock(&watch_lock);
                    path.assign(wd_path_map[evp->wd]);
                    pthread_rwlock_unlock(&watch_lock);

                    path.append(evp->name);
                    conveyor.push_back(path);
                    num++;
                } else {
                    // TODO
                }
            }
        }
    } while (true);

    return num;
}

static void *bar(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread bar created");

    int inot_fd = (int)(intptr_t)arg, n, ret;
    struct pollfd pfd = { inot_fd, POLLIN | POLLPRI, 0 };

    do {
        //n = poll(&pfd, 1, -1);
        //n = poll(&pfd, 1, 300000);
        n = poll(&pfd, 1, 120000);
        if (n == -1) {
            if (errno == EINTR) {
                write_log(app_log_path, LOG_WARNING,
                          "poll interrupted by a signal");
                continue;
            }
            write_log(app_log_path, LOG_ERR,
                      "poll failed with errno[%d]", errno);
            write_log(app_log_path, LOG_ERR, "thread bar exiting");
            return (void *)-1;
        } else if (n == 0) {
            write_log(app_log_path, LOG_WARNING,
                      "poll timed out after 120 seconds");
            continue;
        }

        if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            write_log(app_log_path, LOG_WARNING, "poll abnormal");
            continue;
        }

        int i;
        for (i = 0; i < 3; i++) {
            if ((ret = pthread_mutex_lock(&conveyor_mtx)) != 0) {
                write_log(app_log_path, LOG_WARNING,
                          "pthread_mutex_lock[conveyor_mtx] failed"
                          " [%d] with errno[%d]", i, ret);
                continue;
            } else {
                break;
            }
        }
        if (i == 3) {
            write_log(app_log_path, LOG_ERR, "thread bar exiting");
            return (void *)-1;
        }
        if ((n = baz(inot_fd)) > 0) {
            pthread_mutex_unlock(&conveyor_mtx);
            pthread_cond_broadcast(&conveyor_cond);
        } else if (n == 0) {
            pthread_mutex_unlock(&conveyor_mtx);
        } else {
            pthread_mutex_unlock(&conveyor_mtx);
            pthread_cond_broadcast(&conveyor_cond);
            write_log(app_log_path, LOG_ERR, "thread bar exiting");
            return (void *)-1;
        }

        if ((n = poll_producer(producer, 1000, 2)) > 0) {
            //write_log(app_log_path, LOG_INFO, "rdkafka poll events[%d] of"
            //          " producer for possible big outq size", n);
        }
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread bar exiting");

    return (void *)0;
}

static void persist_offsets()
{
    if (offset_archive_path == NULL) {
        write_log(app_log_path, LOG_WARNING, "no path for storing offsets");
        return;
    }

    FILE *fp = fopen(offset_archive_path, "w");
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR, "fopen[%s] failed with errno[%d]",
                  offset_archive_path, errno);
        return;
    }

    pthread_rwlock_rdlock(&offset_lock);

    char temp[128];
    //std::map<std::string, FileOffset, PathComp>::iterator it =
    //    path_offset_table.begin();
    std::map<std::string, FileOffset>::iterator it = path_offset_table.begin();

    while (it != path_offset_table.end()) {
#if 0
        // XXX
        if (strncmp(it->first.c_str(), LOG_PATH_ROOT, LOG_PATH_ROOT_LEN) != 0) {
            write_log(app_log_path, LOG_WARNING, "invalid log file[%s/%s]",
                      it->first.c_str(), it->second.filename);
            it++;
            continue;
        }
#endif
        snprintf(temp, sizeof(temp), "%s/%s:%ld\n", it->first.c_str(),
                 it->second.filename, it->second.offset);
        int n = fputs(temp, fp);
        if (n == EOF) {
            write_log(app_log_path, LOG_ERR,
                      "fputs[%s] for persisting offsets failed", temp);
        }
        it++;
    }

    pthread_rwlock_unlock(&offset_lock);

    fclose(fp);
}

static void *archive_offsets(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread archive_offsets created");

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_detach[archive_offsets]"
                  " failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread archive_offsets exiting");
        return (void *)-1;
    }

    do {
        sleep(60);
        persist_offsets();
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread archive_offsets exiting");

    return (void *)0;
}

#if 0
#define BACKTRACE_PATH (CWD_ROOT "/log_aggregator.backtrace")
#define ADDRESS_SIZE 64

static void record_backtrace()
{
    int fd = open(BACKTRACE_PATH, O_WRONLY | O_CREAT,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
    if (fd == -1) {
        write_log(app_log_path, LOG_ERR, "open[%s] failed with errno[%d]",
                  BACKTRACE_PATH, errno);
        return;
    }

    int n;
    void *buf[ADDRESS_SIZE];

    n = backtrace(buf, ADDRESS_SIZE);
    if (n == ADDRESS_SIZE) {
        write_log(app_log_path, LOG_WARNING,
                  "addresses of stack may have been truncated");
    }
    backtrace_symbols_fd(buf, n, fd);

    close(fd);
}
#endif

// When I wrote this, only God and I understood what I was doing
// Now, God only knows

static void clear_up_wd_maps(int inot_fd)
{
    std::deque<std::string>::iterator conveyor_it;
    std::vector<std::map<int, std::string>::iterator> its, its2;
    int ret;

    for (std::map<int, std::string>::iterator it = wd_path_map.begin(),
         it2 = wd_topic_map.begin(); it != wd_path_map.end(); it++, it2++) {
        if (it->second.find_first_of('/', LOG_PATH_ROOT_LEN + 1)
            == it->second.find_last_of('/')) {
            continue;
        }
        if (it->second.find(today_ymd) != std::string::npos) continue;

        if ((ret = pthread_mutex_lock(&conveyor_mtx)) != 0) {
            write_log(app_log_path, LOG_WARNING,
                      "pthread_mutex_lock[conveyor_mtx] failed with errno[%d]",
                      ret);
            continue;
        }
        if (conveyor.empty()) {
            pthread_mutex_unlock(&conveyor_mtx);
            int ret = inotify_rm_watch(inot_fd, it->first);
            if (ret == -1) {
                write_log(app_log_path, LOG_ERR,
                          "inotify_rm_watch[%s] for update failed"
                          " with errno[%d]", it->second.c_str(), errno);
            }
            its.push_back(it);
            its2.push_back(it2);
            continue;
        }
        for (conveyor_it = conveyor.begin();
             conveyor_it != conveyor.end(); conveyor_it++) {
            if (conveyor_it->find(it->second) != std::string::npos) break;
        }
        pthread_mutex_unlock(&conveyor_mtx);

        if (conveyor_it == conveyor.end()) {
            int ret = inotify_rm_watch(inot_fd, it->first);
            if (ret == -1) {
                write_log(app_log_path, LOG_ERR,
                          "inotify_rm_watch[%s] for update failed "
                          "with errno[%d]", it->second.c_str(), errno);
            }
            its.push_back(it);
            its2.push_back(it2);
        }
    }

    for (size_t i = 0, j = its.size(); i < j; i++) {
        std::map<int, std::string>::iterator& it = its[i];
        write_log(app_log_path, LOG_INFO,
                  "erasing wd[%d] path[%s] from wd_path_map",
                  it->first, it->second.c_str());
        wd_path_map.erase(it);
    }
    for (size_t i = 0, j = its2.size(); i < j; i++) {
        std::map<int, std::string>::iterator& it = its2[i];
        write_log(app_log_path, LOG_INFO,
                  "erasing wd[%d] topic[%s] from wd_topic_map",
                  it->first, it->second.c_str());
        wd_topic_map.erase(it);
    }
}

static void clear_up_offset_table()
{
    std::string topic;
    std::deque<std::string>::iterator conveyor_it;
    std::vector<std::map<std::string, FileOffset>::iterator> its;
    int ret;

    for (std::map<std::string, FileOffset>::iterator it =
         path_offset_table.begin(); it != path_offset_table.end(); it++) {
        if (it->first.find(today_ymd) != std::string::npos) continue;

        if (!extract_topic_from_path(it->first, topic)) continue;

        if ((ret = pthread_mutex_lock(&conveyor_mtx)) != 0) {
            write_log(app_log_path, LOG_WARNING,
                      "pthread_mutex_lock[conveyor_mtx] failed with errno[%d]",
                      ret);
            continue;
        }
        if (conveyor.empty()) {
            pthread_mutex_unlock(&conveyor_mtx);
            its.push_back(it);
            continue;
        }
        for (conveyor_it = conveyor.begin();
             conveyor_it != conveyor.end(); conveyor_it++) {
            if (conveyor_it->find(it->first) != std::string::npos) break;
        }
        pthread_mutex_unlock(&conveyor_mtx);

        if (conveyor_it == conveyor.end()) its.push_back(it);
    }

    for (size_t i = 0, j = its.size(); i < j; i++) {
        write_log(app_log_path, LOG_INFO, "erasing old key[%s] "
                  "from path_offset_table", its[i]->first.c_str());
        path_offset_table.erase(its[i]);
    }
}

static void get_zero_ts(time_t ts)
{
    struct tm tm;

    localtime_r(&ts, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    zero_ts = mktime(&tm);
}

static void *zero_update(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread zero_update created");

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_detach[zero_update] failed"
                  " with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread zero_update exiting");
        return (void *)-1;
    }

    int inot_fd = (int)(intptr_t)arg;

    do {
        // XXX
        sleep(15);

        time_t ts = time(NULL);
        std::string ymd = ymd_stringify(ts);
        if (ymd == today_ymd) {
            time_t diff = zero_ts + 86400 - ts;
            if (diff < 15) {
                sleep(diff + 1);
                ts += diff + 1;
                ymd = ymd_stringify(ts);
                zero_ts += 86400;
            } else {
                continue;
            }
        } else {
            get_zero_ts(ts);
        }

        // step 1: update date ymd
        yesterday_ymd = today_ymd;
        today_ymd = ymd;

        write_log(app_log_path, LOG_INFO, "updating some global data with"
                  " yesterday[%s] & today[%s]",
                  yesterday_ymd.c_str(), today_ymd.c_str());

        // step 2: update normal_paths
        normal_paths.clear();
        normalize_paths();

        // step 3: update wd_path_map & wd_topic_map
        // XXX one of inotify bugs may be triggered in extremely low probability
        pthread_rwlock_wrlock(&watch_lock);
        clear_up_wd_maps(inot_fd);
        for (std::multimap<std::string, std::string>::const_iterator it =
             normal_paths.begin(); it != normal_paths.end(); it++) {
            if (it->second.find(today_ymd) == std::string::npos) {
                continue;
            }

            int wd = inotify_add_watch(inot_fd, it->second.c_str(),
                                      IN_MOVED_TO | IN_CREATE | IN_DONT_FOLLOW);
            if (wd == -1) {
                int errno_sv = errno;
                if (errno_sv != ENOENT) {
                    write_log(app_log_path, LOG_ERR, "inotify_add_watch[%s]"
                              " for zero update failed with errno[%d]",
                              it->second.c_str(), errno_sv);
                    close(inot_fd);
                    write_log(app_log_path, LOG_ERR,
                              "thread zero_update exiting");
                    return (void *)-1;
                }
                write_log(app_log_path, LOG_INFO, "inotify_add_watch[%s]"
                          " for zero update failed with errno[%d]",
                          it->second.c_str(), errno_sv);
            } else {
                wd_path_map[wd] = it->second;
                wd_topic_map[wd] = it->first;
            }
        }
        pthread_rwlock_unlock(&watch_lock);

        // step 4: update path_offset_table
        pthread_rwlock_wrlock(&offset_lock);
        clear_up_offset_table();
        pthread_rwlock_unlock(&offset_lock);
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread zero_update exiting");

    return (void *)0;
}

static void *routine_update(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread routine_update created");

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_detach[routine_update]"
                  " failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread routine_update exiting");
        return (void *)-1;
    }

    int inot_fd = (int)(intptr_t)arg;

    do {
        sleep(4027); // 1 hour + 7 minute + 7 second

        // every four hours
        if (time(NULL) - zero_ts < 14400) continue;

        // step 1: update wd_path_map & wd_topic_map
        pthread_rwlock_wrlock(&watch_lock);
        clear_up_wd_maps(inot_fd);
        pthread_rwlock_unlock(&watch_lock);

        // step 2: update path_offset_table
        pthread_rwlock_wrlock(&offset_lock);
        clear_up_offset_table();
        pthread_rwlock_unlock(&offset_lock);
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread routine_update exiting");

    return (void *)0;
}

static void *remedy(void *arg)
{
    char *buf = (char *)arg, *p;

    write_log(app_log_path, LOG_INFO, "thread remedy[%s] created", buf);

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR,
                  "pthread_detach[remedy] failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR,
                  "thread remedy[%s] exiting prematurely", buf);
        delete buf;
        return (void *)-1;
    }

    // XXX
    if (strncmp(buf, LOG_PATH_ROOT, LOG_PATH_ROOT_LEN) != 0) {
        write_log(app_log_path, LOG_WARNING,
                  "invalid log file[%s] for remedy", buf);
        write_log(app_log_path, LOG_ERR,
                  "thread remedy[%s] exiting prematurely", buf);
        delete buf;
        return (void *)-1;
    }

    if ((p = strrchr(buf, ':')) == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "invalid offset record[%s] for remedy", buf);
        write_log(app_log_path, LOG_ERR,
                  "thread remedy[%s] exiting prematurely", buf);
        delete buf;
        return (void *)-1;
    }
    *p = '\0';

    long offset = atol(p + 1);
    char topic[32];
    size_t l;

    p = strchr(buf + LOG_PATH_ROOT_LEN, '/');
    l = p - (buf + LOG_PATH_ROOT_LEN);
    memcpy(topic, buf + LOG_PATH_ROOT_LEN, l);
    topic[l] = '\0';

    int num = produce_msgs_and_save_offset(topics[topic], buf, offset);

    write_log(app_log_path, LOG_INFO, "thread remedy[%s][%d] is leaving",
              buf, num);
    delete buf;

    return (void *)0;
}

static int check_and_remedy()
{
    if (offset_archive_path == NULL) {
        write_log(app_log_path, LOG_WARNING, "no offset path for remedy");
        return 0;
    }

    int num = 0;
    struct stat st;

    if (stat(offset_archive_path, &st) == 0) {
        if (st.st_size == 0) return 0;
        write_log(app_log_path, LOG_INFO, "remedy-ing unproduced messages");
        FILE *fp = fopen(offset_archive_path, "r");
        if (fp == NULL) {
            write_log(app_log_path, LOG_ERR, "fopen[%s] for remedy failed"
                      " with errno[%d]", offset_archive_path, errno);
            return -1;
        }
        char buf[128];
        int ret;
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            pthread_t thr;
            // each line must have been tailed with a '\n'
            size_t l = strlen(buf);
            char *s = new char[l];
            // replace '\n' with '\0'
            buf[l - 1] = '\0';
            memcpy(s, buf, l);
            if ((ret = pthread_create(&thr, NULL, remedy, (void *)s)) != 0) {
                write_log(app_log_path, LOG_ERR, "pthread_create[remedy] failed"
                          " with errno[%d]", ret);
                delete s;
                fclose(fp);
                exit(EXIT_FAILURE);
            }
            num++;
        }
        fclose(fp);
    } else {
        if (errno != ENOENT) {
            write_log(app_log_path, LOG_ERR, "stat[%s] failed with errno[%d]",
                      offset_archive_path, errno);
            exit(EXIT_FAILURE);
        }
    }

    return num;
}

static void handle_sigs(int dummy)
{
    (void)dummy;
    persist_offsets();
    //record_backtrace();
    exit(EXIT_FAILURE);
}

static void set_sig_handlers()
{
    signal(SIGTERM, handle_sigs);
    signal(SIGINT, handle_sigs);
    //signal(SIGABRT, handle_sigs);
    //signal(SIGQUIT, handle_sigs);
    //signal(SIGSEGV, handle_sigs);
    //signal(SIGBUS, handle_sigs);
}

// TODO thread failover
int main(int argc, char *argv[])
{
    int opt;
    char *conf_path;

    do {
        opt = getopt(argc, argv, "c:b:p:a:l:");
        switch (opt) {
        case 'c':
            conf_path = optarg;
            fprintf(stderr, "log_aggregator conf path: %s\n", conf_path);
            break;
        case 'b':
            brokers = optarg;
            fprintf(stderr, "meta brokers: %s\n", brokers);
            break;
        case 'p':
            producer_conf = optarg;
            fprintf(stderr, "Kafka producer conf path: %s\n", producer_conf);
            break;
        case 'a': // file for storing offsets
            offset_archive_path = optarg;
            fprintf(stderr, "log_aggregator offset storage path: %s\n",
                    offset_archive_path);
            break;
        case 'l':
            app_log_path = optarg;
            fprintf(stderr, "log_aggregator log path: %s\n", app_log_path);
            break;
        default:
            // usage
            fprintf(stderr, "Usage: ./log_aggregator -c -b -p -a\n");
            //exit(EXIT_FAILURE);
            continue;
        }
    } while (opt != -1);

    parse_conf_file(conf_path);
    if (raw_paths.size() == 0) {
        fprintf(stderr, "retrieval of configured paths failed\n");
        exit(EXIT_FAILURE);
    }
#if 1
for (std::map<std::string, std::string>::iterator it = raw_paths.begin();
     it != raw_paths.end(); it++) {
    fprintf(stderr, "topic[%s] raw path[%s]\n",
            it->first.c_str(), it->second.c_str());
}
#endif

    time_t ts = time(NULL);
    yesterday_ymd = ymd_stringify(ts - 86400);
    today_ymd = ymd_stringify(ts);
    get_zero_ts(ts);

    normalize_paths();
    if (normal_paths.size() == 0) {
        write_log(app_log_path, LOG_ERR, "normalize_paths failed");
        exit(EXIT_FAILURE);
    }
#if 1
for (std::multimap<std::string, std::string>::iterator it =
     normal_paths.begin(); it != normal_paths.end(); it++) {
    fprintf(stderr, "topic[%s] normal path[%s]\n",
            it->first.c_str(), it->second.c_str());
}
#endif

    if ((producer = create_kafka_producer(producer_conf, brokers)) == NULL) {
        write_log(app_log_path, LOG_ERR, "create_kafka_producer to brokers[%s]"
                  " with conf[%s] failed", brokers, producer_conf);
        exit(EXIT_FAILURE);
    }

    if (!init_topics()) {
        destroy_kafka_producer(producer);
        exit(EXIT_FAILURE);
    }

    if (check_and_remedy() < 0) {
        destroy_topics();
        destroy_kafka_producer(producer);
        exit(EXIT_FAILURE);
    }

    int inot_fd = preprocess_inotify();
    if (inot_fd == -1) {
        destroy_topics();
        destroy_kafka_producer(producer);
        exit(EXIT_FAILURE);
    }
#if 1
for (std::map<int, std::string>::iterator it = wd_path_map.begin();
     it != wd_path_map.end();
     it++) {
    fprintf(stderr, "wd[%d] path[%s]\n", it->first, it->second.c_str());
}
#endif

    set_sig_handlers();

    pthread_t thr2;
    int ret = pthread_create(&thr2, NULL, bar, (void *)(intptr_t)inot_fd);
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_create[bar] failed"
                  " with errno[%d]", ret);
        destroy_topics();
        destroy_kafka_producer(producer);
        close(inot_fd);
        exit(EXIT_FAILURE);
    }

    sleep(60);

    pthread_t thr;
    if ((ret = pthread_create(&thr, NULL, foo, NULL)) != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_create[foo] failed"
                  " with errno[%d]", ret);
        destroy_topics();
        destroy_kafka_producer(producer);
        close(inot_fd);
        exit(EXIT_FAILURE);
    }

    pthread_t thr3;
    ret = pthread_create(&thr3, NULL, archive_offsets, NULL);
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_create[archive_offsets]"
                  " failed with errno[%d]", ret);
        destroy_topics();
        destroy_kafka_producer(producer);
        close(inot_fd);
        exit(EXIT_FAILURE);
    }

    pthread_t thr4;
    ret = pthread_create(&thr4, NULL, handle_conveyor_backlog, NULL);
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR,
                  "pthread_create[handle_conveyor_backlog] failed"
                  " with errno[%d]", ret);
        destroy_topics();
        destroy_kafka_producer(producer);
        close(inot_fd);
        exit(EXIT_FAILURE);
    }

    pthread_t thr5;
    ret = pthread_create(&thr5, NULL, zero_update, (void *)(intptr_t)inot_fd);
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_create[zero_update] failed"
                  " with errno[%d]",
                  ret);
        destroy_topics();
        destroy_kafka_producer(producer);
        close(inot_fd);
        exit(EXIT_FAILURE);
    }

    pthread_t thr6;
    ret = pthread_create(&thr6, NULL, routine_update,
                         (void *)(intptr_t)inot_fd);
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_create[routine_update] failed"
                  " with errno[%d]", ret);
        destroy_topics();
        destroy_kafka_producer(producer);
        close(inot_fd);
        exit(EXIT_FAILURE);
    }

    if ((ret = pthread_join(thr2, NULL)) != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_join[bar] failed"
                  " with errno[%d]", ret);
    }

    destroy_topics();
    destroy_kafka_producer(producer);
    close(inot_fd);

    write_log(app_log_path, LOG_WARNING, "unexpected exiting");

    exit(EXIT_FAILURE);
}
