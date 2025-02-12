#!/usr/bin/env python
import re
import os
import sys
import time
import socket
import requests
import json
import logging
import asyncio
import fnmatch
import yaml
from glob import glob
from collections import defaultdict
from threading import Thread

from queuey import Queuey
from loghandlers_realtime import LogItemCount, LogItemAvg, LogItemAvgSumStdev

try:
    import docker
except:
    class docker:
        def from_env():
            return None

class Base:
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.running = True
        self.jobs = {}

    async def stop(self):
        self.log.info(f"Stopping the whole {self.__class__.__name__} name {self.name}.")
        self.running = False
        for name, task in self.jobs.items():
            self.log.info(f"Stopping {name} coroutine.")
            task.cancel()

class LogFollower(Base):
    def __init__(self, in_q, manage_q):
        super().__init__()
        self.name = "main"
        self.in_q = in_q
        self.manage_q = manage_q
        self.dockersglobmeta = defaultdict(list)
        self.filesglobmeta = defaultdict(list)
        self.filemeta = defaultdict(self._create_filemeta)

        self.filemetalock = asyncio.Semaphore()


    async def initialize_docker_client(self):
        init_retry_delay = 60
        tries = 1
        tries_limit = 1000
        while True:
            if tries > tries_limit:
                self.log.error(f"Could not initialize docker client after {tries} tries. Aborting.")
                return False
            try:
                self.log.info(f"Initializing docker client. Try number: {tries}. Retry limit: {tries_limit}. Retry delay {init_retry_delay}s")
                self.docker_client = docker.from_env()
                if self.docker_client is None:
                    self.log.error(f"Docker package is not present. Aborting.")
                    return False
                self.log.info(f"Initialized docker client succesfully. Try nubmer {tries}.")
                return True
            except Exception as e:
                self.log.warning(f"Initializing docker client failed. Try number: {tries}. Retry delay {init_retry_delay}s. Exception occured: {e}")
            await asyncio.sleep(init_retry_delay)
            tries += 1


    @staticmethod
    def _create_filemeta():
        return {"qs": [], "identifier": None, "task": None, "f": None, "stat": None, "tries": 0}
    
    def _updatefilemeta(self, file, qs, identifier):
        self.filemeta[file]["qs"].extend(
                [q for q in qs if q not in self.filemeta[file]["qs"]]
        )
        self.filemeta[file]["identifier"] = identifier

    async def follower(self, identifier, f, qs):
        ino = os.stat(f.fileno()).st_ino
        self.log.info(f"Follower coro created for file {f.name} inode {ino}.")
        while True:
            try:
                line = f.readline()
                await asyncio.sleep(0)
                if not line:
                    await asyncio.sleep(0.5)
                    continue
                item = {"rx_ts": round(time.time() * 1000), "identifier": identifier, "line": line}
                for q in qs:
                    q.put(item)
                await asyncio.sleep(0)
            except UnicodeDecodeError as e:
                self.log.warning(f"During the line decoding error occured. Continue to next line. Error '{e}'.")
                continue
            except asyncio.exceptions.CancelledError:
                self.log.info(f"Follower coro cancelled for file {f.name} inode {ino}.")
                f.close()
                return

    async def prune_filemeta(self, file):
        async with self.filemetalock:
            try:
                self.filemeta[file]["task"].cancel()
            except (AttributeError, KeyError):
                pass
            try:
                del self.filemeta[file]
            except (AttributeError, KeyError):
                pass
            self.log.info(f"Filemeta items deleted for file: '{file}'.")

    async def watch_followers(self):
        try:
            await asyncio.sleep(1)
            delay = 60
            self.log.info(f"File followers watcher job started. Recheck interval: '{delay}s'.")
            while True:
                async with self.filemetalock:
                    self.log.info(f"Acquired filemeta lock. Inspecting files and followers.")
                    for file, meta in self.filemeta.items():
                        tries_limit = 2
                        self.log.info(f"Inspecting. Tries with failure: '{meta['tries']}'. File '{file}'.")
                        if meta["tries"] >= tries_limit:
                            self.log.info(f"File is absent. Scheduling for deletion from filemeta. File: '{file}'.")
                            asyncio.create_task(self.prune_filemeta(file))
                            continue
                        try:
                            stat = os.stat(file)
                            meta.setdefault("stat", stat)
                            if meta["f"] is None:
                                meta["f"] = open(file,'r')
                                meta["f"].seek(0, 2)
                                meta["task"] = asyncio.create_task(self.follower(meta["identifier"], meta["f"], meta["qs"]))
                                self.log.info(f"File opened. Created file follower task.")
                            elif stat.st_ino != meta["stat"].st_ino:
                                meta["task"].cancel()
                                meta["f"] = open(file,'r')
                                meta["task"] = asyncio.create_task(self.follower(meta["identifier"], meta["f"], meta["qs"]))
                                self.log.info(f"File inode changed. Recreated logfollower task.")
                            elif stat.st_size < meta["stat"].st_size:
                                meta["f"].seek(0)
                                self.log.warning(f"File size reduced. Detected (copy)truncate. Seek to the beginning.")
                            meta["stat"] = stat
                            meta["tries"] = 0
                        except FileNotFoundError:
                            meta["tries"] += 1
                            self.log.warning(f"File not found. Incremented failure tries count. Continue.")
                            continue
                        self.log.info(f"File check done.")
                self.log.info(f"Released filemeta lock. Inspecting files and followers done.")
                await asyncio.sleep(delay)
        except asyncio.exceptions.CancelledError:
            try:
                for meta in self.filemeta.values():
                    meta["task"].cancel()
            except AttributeError:
                pass
            self.log.info(f"File followers watcher job stopped due to task cancellation. All followers cancelled.")
            return

    async def watch_paths_glob(self):
        try:
            delay = 10
            self.log.info(f"Paths glob checker started. Recheck interval: '{delay}s'.")
            while True:
                for paths, qs in self.filesglobmeta.items():
                    for path in paths:
                        await asyncio.sleep(0)
                        for file in glob(path):
                            self._updatefilemeta(file, qs, file)
                await asyncio.sleep(delay)
        except asyncio.exceptions.CancelledError:
            return


    async def watch_dockers_glob(self):
        try:
            delay = 60
            err_delay = 1
            prefix = '/var/lib/docker/containers'
            self.log.info(f"Docker glob checker started. Recheck interval: '{delay}s'.")
            while True:
                try:
                    all_containers = {c.name: c.id for c in self.docker_client.containers.list()}
                except (AttributeError, requests.exceptions.ConnectionError) as e:
                    self.log.info(f"Exception occured: {e}. Creating new docker client.")
                    if not await self.initialize_docker_client():
                        self.log.error(f"Docker glob checker stopped due to unavailable docker client.")
                        return
                    continue
                except docker.errors.NotFound as e:
                    self.log.warning(f"Docker api error occured during api call. Retry in '{err_delay}s'. Error: '{e}'.")
                    await asyncio.sleep(err_delay)
                    continue
                except Exception as e:
                    self.log.error(f"Exeption occured: {e}. Docker glob checker stopped due to an unknown error.")
                    return
                for names, qs in self.dockersglobmeta.items():
                    containers = []
                    for name in names:
                        containers.extend(fnmatch.filter(all_containers.keys(), name))
                    for container in containers:
                        container_hash = all_containers[container]
                        file = "/".join([prefix, all_containers[container], all_containers[container]]) + '-json.log'
                        self._updatefilemeta(file, qs, container)
                await asyncio.sleep(delay)
        except asyncio.exceptions.CancelledError:
            self.log.info(f"Docker glob checker stopped due to task cancellation.")
            return


    async def add_watch_paths(self, meta, q):
        if q not in self.filesglobmeta[meta]:
            self.filesglobmeta[meta].append(q)
        if not self.jobs.get('watch_paths_glob', False):
            self.jobs['watch_paths_glob'] = asyncio.create_task(self.watch_paths_glob())
        if not self.jobs.get('watch_followers', False):
            self.jobs['watch_followers'] = asyncio.create_task(self.watch_followers())

    async def add_watch_dockers(self, meta, q):
        if q not in self.dockersglobmeta[meta]:
            self.dockersglobmeta[meta].append(q)
        if not self.jobs.get('watch_dockers_glob', False):
            self.jobs['watch_dockers_glob'] = asyncio.create_task(self.watch_dockers_glob())
        if not self.jobs.get('watch_followers', False):
            self.jobs['watch_followers'] = asyncio.create_task(self.watch_followers())

    async def listen_input(self):
        while True:
            try:
                item = await self.in_q.get()
            except asyncio.exceptions.CancelledError:
                break
            name, meta, q = item
            if name == 'docker':
                await self.add_watch_dockers(meta, q)
            elif name == 'path':
                await self.add_watch_paths(meta, q)

    async def run(self):
        self.jobs['listen_input'] = asyncio.create_task(self.listen_input())
        res = await self.manage_q.get()
        if res:
            await self.stop()

            

class LogEntryHandler(Base):

    def __init__(self, logf_q, manage_q, config):
        super().__init__()
        self.logf_q = logf_q
        self.manage_q = manage_q
        self.running = True
        self.name = config["name"]
        self.dockers = frozenset(config.get("dockers",[]))
        self.paths = frozenset(config.get("paths", []))
        self.regex = config["line_match_regex"]
        self.re_mutate_id = config.get("re_mutate_id", False)
        self.single = config.get("single_metric", False)
        self.labels = config.get("labels", {})
        self.q = Queuey()
        self.url = "http://127.0.0.1:8429/api/v1/import"
        if not self.labels.get("hostname"):
            self.labels.update({"hostname": socket.gethostname()})
        self.actions = {"Count": LogItemCount, "Avg": LogItemAvg, "AvgSumStdev": LogItemAvgSumStdev}
        self.data = defaultdict(self.actions[config["action"]])

    def create_matcher(self):
        def single(x):
            return 'single'
        def multi(x):
            return x
        if self.single:
            return single
        else:
            return multi

    def create_re_mutate_id(self):
        def original(x):
            return x
        def mutated(x):
            res = re.search(self.re_mutate_id, x)
            if res:
              return res.group("value")
            else:
              return x
        if self.re_mutate_id:
            return mutated
        else:
            return original

    @staticmethod
    def _re_labels(regex, identifier):
        m = None
        if regex:
            m = re.search(regex, identifier)
        if m:
            return m.groupdict()
        return {}

    async def consumer(self):
        try:
            matcher = self.create_matcher()
            mutator = self.create_re_mutate_id()
            self.log.info(f"Started searching '{self.regex}' in '{self.paths}', '{self.dockers}'.")
            regex = re.compile(self.regex)
            while True:
                try:
                    item = await self.q.get()
                    res = regex.search(item["line"])
                    if res:
                        self.log.debug(f"Found match. Re: '{regex}', result: '{res.groups()}'.")
                        item.update({"value": res.group("value")})
                        key = matcher(mutator(item["identifier"]))
                        self.data[key].append(item)
                    await asyncio.sleep(0)
                except asyncio.exceptions.CancelledError:
                    break
        except asyncio.exceptions.CancelledError:
            return 

    async def counter(self):
        self.log.info(f"Started counter for '{self.name}'.")
        try:
            while True:
                await asyncio.sleep(1)
                for key in list(self.data.keys()):
                    await self.data[key].calculate()
        except asyncio.exceptions.CancelledError:
            return 

    async def dump_and_send(self, re_id):
            try:
                json_lines = []
                for identifier, item in self.data.items():
                    await asyncio.sleep(0)
                    _labels = {"identifier": identifier}
                    if re_id:
                        _labels = self._re_labels(re_id, identifier)
                    content = item.dump()
                    json_lines.append({"metric": {"__name__": self.name, **self.labels, **_labels}, "values": content["values"], "timestamps": content["timestamps"] })
                json_string = "\n".join([json.dumps(line) for line in json_lines])
                self.log.debug(f"Sending '{json_string}' to '{self.url}'.")
                try:
                    requests.post(self.url, data=json_string)
                    for item in self.data.values():
                        item.clear()
                except Exception as e:
                    self.log.error(f"Exception occured during sending json data: '{e}'.")
            except Exception as e:
                self.log.error(f"Exception occured: '{e}'.")
                return 

    async def dumper(self):
        try:
            self.log.info(f"Started dumper for '{self.name}'.")
            re_id = self.labels.pop("re_identifier", None)
            while True:
                await asyncio.sleep(10)
                await self.dump_and_send(re_id)
        except asyncio.exceptions.CancelledError:
            await self.dump_and_send(re_id)


    async def run(self):
        if self.paths:
            self.logf_q.put(('path', self.paths, self.q))
        if self.dockers:
            self.logf_q.put(('docker', self.dockers, self.q))
        self.jobs['consumer'] = asyncio.create_task(self.consumer())
        self.jobs['counter'] = asyncio.create_task(self.counter())
        self.jobs['dumper'] = asyncio.create_task(self.dumper())
        res = await self.manage_q.get()
        if res:
            await self.stop()


def load_config():
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error opening the config file.\nException occured: {e}")
        sys.exit(1)


async def prep_logh(logf_in_q, manage_q, config):
    loghs = []

    for cfg in config:
        logh = LogEntryHandler(logf_in_q, manage_q, cfg)
        loghs.append(logh.run())

    _ = await asyncio.gather(*loghs)

def run_logh(logf_in_q, manage_q, config):
    asyncio.run(prep_logh(logf_in_q, manage_q, config))

def run_logf(logfollower):
    asyncio.run(logfollower.run())

def main():
    config = load_config()
    logf_in_q = Queuey()
    manage_q = Queuey()

    logfollower = LogFollower(logf_in_q, manage_q)

    t1 = Thread(target=run_logf, args=(logfollower,))
    t2 = Thread(target=run_logh, args=(logf_in_q, manage_q, config))
    t1.start()
    t2.start()


    while True:
        try:
            time.sleep(3600)
        except KeyboardInterrupt:
            break
    for _ in range(manage_q.len_getters()):
        manage_q.put(True)
    t1.join()
    t2.join()
            

if __name__ == "__main__":

    logger = logging.getLogger(__name__)
    FORMAT = "[%(asctime)s %(levelname)s %(process)d %(filename)s:%(lineno)s in %(name)s:%(funcName)s]: %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    main()
