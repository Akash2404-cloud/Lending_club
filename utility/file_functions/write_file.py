from utility.logger import *

class write_file:

    def __init__(self , loc , data_format):
        self.loc = loc
        self.data_format = data_format

    def file_writer(self, df , mode):
        df.repartition(1).write \
        .format(self.data_format) \
        .mode(mode) \
        .option('header' , 'true') \
        .option('path', self.loc) \
        .save()
        logger.info('file saved successfully to location {}'.format(self.loc))

    # def files_writer(self , ):
