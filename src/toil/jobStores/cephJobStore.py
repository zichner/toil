# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from contextlib import contextmanager
import os
import uuid
from StringIO import StringIO
from bd2k.util.threading import ExceptionalThread
from boto.exception import S3ResponseError as CephResponseError, S3ResponseError
import boto.s3
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
import cPickle
import itertools

from toil.job import JobException
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)
from toil.jobWrapper import JobWrapper


class CephJobStore(AbstractJobStore):

    @classmethod
    def loadOrCreateJobStore(cls, jobStoreString, config=None, **kwargs):
        namePrefix = jobStoreString
        return cls(namePrefix, config=config, **kwargs)

    def __init__(self, namePrefix, config=None):
        self.ceph = self._connectCeph()
        self.bucketName = namePrefix
        self.bucket = None
        exists = True

        try:
            self.bucket = self.ceph.get_bucket(self.bucketName, validate=True)
        except:
            exists = False

        create = config is not None
        self._checkJobStoreCreation(create, exists, namePrefix)

        if not exists:
            self.bucket = self.ceph.create_bucket(self.bucketName)

        super(CephJobStore, self).__init__(config=config)

        self.statsPrefix = '265f64b4-365c-4bde-a349-5912a0634174'
        self.readStatsPrefix = '_'+self.statsPrefix

    def _connectCeph(self):
        return boto.connect_s3()

    @classmethod
    def _readFromUrl(cls, url, writable):
        raise NotImplementedError()

    @classmethod
    def _writeToUrl(cls, readable, url):
        raise NotImplementedError()

    @classmethod
    def _supportsUrl(cls, url):
        raise NotImplementedError()

    def deleteJobStore(self):
        for item in self.bucket.list():
            item.delete()
        try:
            self.bucket.delete()
        except S3ResponseError as e:
            if e.status == '409':
                self.deleteJobStore()

    def create(self, command, memory, cores, disk, predecessorNumber=0):
        jobStoreID = self._newJobStoreID()
        job = JobWrapper(jobStoreID=jobStoreID,
             command=command, memory=memory, cores=cores, disk=disk,
             remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
             predecessorNumber=predecessorNumber)
        self._writeFile(jobStoreID, StringIO(cPickle.dumps(job)))
        return job

    def _newJobStoreID(self):
        return "job"+str(uuid.uuid4())

    def _newJobStoreFileID(self, jobStoreID=None):
        jobStoreFileID = str(uuid.uuid4())
        if jobStoreID:
            jobStoreFileID = jobStoreID + jobStoreFileID
        return jobStoreFileID

    def _writeFile(self, name, file):
        key = self.bucket.new_key(key_name=name)
        key.set_contents_from_file(file)

    def _readFile(self, name):
        key = self.bucket.get_key(key_name=name, validate=True)
        if key is None:
            raise NoSuchFileException(name)
        return key.get_contents_as_string()

    def exists(self, jobStoreID):
        return self.bucket.new_key(jobStoreID).exists()

    def getPublicUrl(self, fileName):
        key = self.bucket.get_key(key_name=fileName)
        return key.generate_url(expires_in=self.publicUrlExpiration.total_seconds())

    def getSharedPublicUrl(self, sharedFileName):
        self.getPublicUrl(sharedFileName)

    def load(self, jobStoreID):
        key = self.bucket.get_key(jobStoreID, validate=True)
        if key is None:
            raise NoSuchJobException(jobStoreID)
        else:
            return cPickle.loads(key.get_contents_as_string())

    def update(self, job):
        self._writeFile(job.jobStoreID, StringIO(cPickle.dumps(job)))

    def delete(self, jobStoreID):
        try:
            self.bucket.get_key(key_name=jobStoreID, validate=True).delete()
            for key in self.bucket.list(prefix=jobStoreID):
                key.delete()
        except AttributeError:
            pass

    def jobs(self):
        # swag
        return map(lambda key: self.load(key.name),
                   filter(lambda key: len(key.name) == 39,
                          list(self.bucket.list(prefix='job'))))

    def writeFile(self, localFilePath, jobStoreID=None):
        jobStoreFileID = self._newJobStoreFileID(jobStoreID)
        with open(localFilePath, 'rb') as f:
            self._writeFile(jobStoreFileID, f)
        return jobStoreFileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        jobStoreFileID = self._newJobStoreFileID(jobStoreID)
        if self.fileExists(jobStoreFileID):
            raise RuntimeError('%s already exists' % jobStoreFileID)
        key = self.bucket.new_key(jobStoreFileID)
        with self._uploadStream(key) as writable:
            yield (writable, jobStoreFileID)

    def getEmptyFileStoreID(self, jobStoreID=None):
        jobStoreFileID = self._newJobStoreFileID(jobStoreID)
        self._writeFile(jobStoreFileID, StringIO(''))
        return jobStoreFileID

    def readFile(self, jobStoreFileID, localFilePath):
        contents = self._readFile(jobStoreFileID)
        with open(localFilePath, 'wb') as f:
            f.write(contents)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        with self.readSharedFileStream(jobStoreFileID) as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        self.delete(jobStoreFileID)

    def fileExists(self, jobStoreFileID):
        key = self.bucket.get_key(jobStoreFileID, validate=True)
        if key is None:
            return False
        else:
            return True

    def updateFile(self, jobStoreFileID, localFilePath):
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)
        with open(localFilePath, 'rb') as f:
            self._writeFile(jobStoreFileID, f)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        key = self.bucket.get_key(jobStoreFileID, validate=True)
        if key is None:
            raise NoSuchFileException(jobStoreFileID)
        with self._uploadStream(key) as writable:
            yield writable

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        key = self.bucket.new_key(sharedFileName)
        with self._uploadStream(key) as writable:
            yield writable

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        key = self.bucket.get_key(sharedFileName, validate=True)
        if key is None:
            raise NoSuchFileException(sharedFileName)
        with self._downloadStream(key) as readable:
            yield readable

    def writeStatsAndLogging(self, statsAndLoggingString):
        statsFileID = self._newJobStoreFileID(self.statsPrefix)
        key = self.bucket.new_key(statsFileID)
        with self._uploadStream(key) as writable:
            writable.write(statsAndLoggingString)

    def readStatsAndLogging(self, callback, readAll=False):
        read = 0
        statsKeys = list(self.bucket.list(prefix=self.statsPrefix))
        for key in statsKeys:
            contents = self._readFile(key.name)
            callback(StringIO(contents))
            read += 1
            self._renameStatsFile(key.name, contents)
        return read

    def _renameStatsFile(self, name, contents):
        readStatsID = self._newJobStoreFileID(self.readStatsPrefix)
        readStatsKey = self.bucket.new_key(readStatsID)
        with self._uploadStream(readStatsKey) as writable:
            writable.write(contents)
        self.deleteFile(name)

    @contextmanager
    def _uploadStream(self, key):
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                # def multipartReader():
                #     buf = readable.read(self.partSize)
                #     upload = self.bucket.initiate_multipart_upload(
                #         key_name=key.name,
                #         headers=headers)
                #     try:
                #         for part_num in itertools.count():
                #             # There must be at least one part, even if the file is empty.
                #             if len(buf) == 0 and part_num > 0: break
                #             upload.upload_part_from_file(fp=StringIO(buf),
                #                                          # S3 part numbers are 1-based
                #                                          part_num=part_num + 1,
                #                                          headers=headers)
                #             if len(buf) == 0: break
                #             buf = readable.read(self.outer.partSize)
                #     except:
                #         upload.cancel_upload()
                #         raise
                #     else:
                #         self.version = upload.complete_upload().version_id
                def reader():
                    buf = readable.read()
                    buf = StringIO(buf)
                    assert buf.len == key.set_contents_from_file(fp=buf)
                    self.version = key.version_id

                thread = ExceptionalThread(target=reader)
                thread.start()
                yield writable
            thread.join()

    @contextmanager
    def _downloadStream(self, key):
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    try:
                        key.get_contents_to_file(writable)
                    finally:
                        writable.close()

                thread = ExceptionalThread(target=writer)
                thread.start()
                yield readable
                thread.join()
