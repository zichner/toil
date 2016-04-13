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
import boto.s3
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection

from toil.job import JobException
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)


class CephJobStore(AbstractJobStore):

    def __init__(self, namePrefix, config=None):
        super(CephJobStore, self).__init__(config=config)
        pass

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
        raise NotImplementedError()

    def create(self, command, memory, cores, disk,
               predecessorNumber=0):
        raise NotImplementedError()

    def exists(self, jobStoreID):
        raise NotImplementedError()

    def getPublicUrl(self, fileName):
        raise NotImplementedError()

    def getSharedPublicUrl(self, sharedFileName):
        raise NotImplementedError()

    def load(self, jobStoreID):
        raise NotImplementedError()

    def update(self, job):
        raise NotImplementedError()

    def delete(self, jobStoreID):
        raise NotImplementedError()

    def jobs(self):
        raise NotImplementedError()

    def writeFile(self, localFilePath, jobStoreID=None):
        raise NotImplementedError()

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        raise NotImplementedError()

    def getEmptyFileStoreID(self, jobStoreID=None):
        raise NotImplementedError()

    def readFile(self, jobStoreFileID, localFilePath):
        raise NotImplementedError()

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        raise NotImplementedError()

    def deleteFile(self, jobStoreFileID):
        raise NotImplementedError()

    def fileExists(self, jobStoreFileID):
        raise NotImplementedError()

    def updateFile(self, jobStoreFileID, localFilePath):
        raise NotImplementedError()

    def updateFileStream(self, jobStoreFileID):
        raise NotImplementedError()

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        raise NotImplementedError()

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        raise NotImplementedError()

    def writeStatsAndLogging(self, statsAndLoggingString):
        raise NotImplementedError()

    def readStatsAndLogging(self, callback, readAll=False):
        raise NotImplementedError()
