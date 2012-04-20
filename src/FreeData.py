import os, sys, time, re, pymarc, tarfile, hashlib, json
from stat import *
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket

class FreeData:
    """A collection of functions to help us package up, document, and send
       a zipped tarball to AWS S3"""

    def __init__(self, config):
        self.config = config 

    # The list of files we'll operate on
    files = []

    def generate_list_of_files(self):
        """Get a list of the files we'll operate on"""
        for f in os.listdir(self.config.get('general', 'dump_path')):
            pathname = os.path.join(self.config.get('general', 'dump_path'), f)
            mode = os.stat(pathname)[ST_MODE]
            if S_ISREG(mode):
                timepoint = time.time() - int(self.config.get('general', 'timepoint'))
                if os.stat(pathname)[ST_MTIME] > timepoint and re.search('mrc',  pathname):
                    self.files.append(pathname)

    def get_total_record_count(self):
        """Loop through the MARC records and sum the count of the records """
        record_count = 0
        for name in self.files:
            reader = pymarc.MARCReader(file(name))
            for record in reader:
                record_count += 1

        self.record_count = record_count
        self.file_count = len(self.files)

    def create_tar(self):
        """Tar up, gzip, and send to S3 """
        # Remove old tarball
        if os.path.exists(self.config.get('general', 'dump_path') + self.config.get('general', 'tarball_name')):
            os.remove(self.config.get('general', 'dump_path') + self.config.get('general', 'tarball_name'))

        # Tar and gzip things up
        tar = tarfile.open(self.config.get('general', 'dump_path') + self.config.get('general', 'tarball_name'), "w:gz")
        for name in self.files:
            tar.add(name)
        tar.close()

    def get_md5_and_size(self):
        """It's handy to have some stats on the files we packaging,
           let's get the size and md5 sum here"""
        tarball_path = self.config.get('general', 'dump_path') + self.config.get('general', 'tarball_name')
        self.tarball_size = os.path.getsize(tarball_path)

        f = open(tarball_path)
        md5 = hashlib.md5()
        while True:
            data = f.read(128)
            if not data:
                break
            md5.update(data)
         
        self.md5_digest = md5.hexdigest()

    def package_stats(self):
        """We've been gathering stats in different methods. Package them and
           write them to disk here"""
        stats_message = {'record_count': self.record_count,
                         'total_files': self.file_count,
                         'size_tarball': self.tarball_size,
                         'tarball_md5': self.md5_digest}

        stats_file = open('stats.json', 'w')
        stats_file.writelines(json.dumps(stats_message))
        stats_file.close()

    def send_to_aws(self):
        """Everything should be packaged now. Let's send it to S3"""

        # Establish connection to AWS and upload tarball
        conn = S3Connection(self.config.get('general', 'aws_key'), self.config.get('general', 'aws_secret_key'))
        bucket = Bucket(conn, self.config.get('general', 'bucket_name'))
        tarball_key = Key(bucket)
        tarball_key.key = self.config.get('general', 'tarball_name') 
        tarball_path = self.config.get('general', 'dump_path') + self.config.get('general', 'tarball_name')
        tarball_key.set_contents_from_filename(tarball_path)

        # Upload the stats file
        tarball_key.key = 'stats.json'
        tarball_key.set_contents_from_filename(self.config.get('general', 'dump_path') + 'stats.json')
        # uncomment the following to make the bucket publicly downloadable
        #bucket.set_acl('public-read', key.key)
