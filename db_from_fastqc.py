#!/usr/bin/env python
import os
import psycopg2
import sys


class Sequence():
    def __init__(self):
        self.filename = None
        self.filetype = None
        self.encoding = None
        self.total_seqs = None
        self.filtered_seqs = None
        self.sequence_length = None
        self.GC_Content_Percent = None
        self.stats = Status()

    def __getInformation(self, lines):
        # Read the fastqc file and gather the information into attributes of
        # this class. For use in population of database or in working with the
        # results in other ways
        for line in lines:
            if line.startswith("Filename"):
                self.filename = " ".join(line.split()[1:])
            elif line.startswith("File type"):
                self.filetype = " ".join(line.split()[2:])
            elif line.startswith("Encoding"):
                self.encoding = " ".join(line.split()[1:])
            elif line.startswith("Total Sequences"):
                self.total_seqs = line.split()[-1]
            elif line.startswith("Filtered Sequences"):
                self.filtered_seqs = line.split()[-1]
            elif line.startswith("Sequence length"):
                self.sequence_length = line.split()[-1]
            elif line.startswith("%GC"):
                self.GC_Content_Percent = line.split()[-1]
            else:
                pass

    def __getStats(self, lines):
        Module_headers = [line for line in lines if line.startswith(">>") and
                          not line.startswith(">>END_MODULE")]

        for line in Module_headers:
            if line.startswith(">>Basic Statistics"):
                self.stats.Overall = line.split()[-1]
            if line.startswith(">>Per base sequence quality"):
                self.stats.PerBaseSequenceQuality = line.split()[-1]
            if line.startswith(">>Per sequence quality"):
                self.stats.PerSequenceQuality = line.split()[-1]
            if line.startswith(">>Per base sequence content"):
                self.stats.PerBaseSequenceContentS = line.split()[-1]
            if line.startswith(">>Per base GC content"):
                self.stats.PerBaseGCContent = line.split()[-1]
            if line.startswith(">>Per sequence GC content"):
                self.stats.PerSequenceGCContent = line.split()[-1]

    def __getStats2(self, lines):
        Module_headers = [line for line in lines if line.startswith(">>") and
                          not line.startswith(">>END MODULE")]

        for line in Module_headers:
            if line.startswith(">>Per base N content"):
                self.stats.PerBaseNContent = line.split()[-1]
            if line.startswith(">>Sequence Length Distribution"):
                self.stats.SequenceLengthDistribution = line.split()[-1]
            if line.startswith(">>Sequence Duplication Levels"):
                self.stats.SequenceDuplicationLevels = line.split()[-1]
            if line.startswith(">>Overrepresented sequences"):
                self.stats.OverrepresentedSequences = line.split()[-1]
            if line.startswith(">>Kmer Content"):
                self.stats.KMerContent = line.split()[-1]

    def populate(self, fastqc_txt):
        try:
            file_handle = open(fastqc_txt, 'r+')
            lines = file_handle.readlines()
            self.__getInformation(lines)
            self.__getStats(lines)
            self.__getStats2(lines)
        except IOError as e:
            print("IOError occurred while attempting to read %s. %s"
                  % (fastqc_txt, str(e)))
        except Exception as e:
            print("Unexpected error(s) while attempting to read %s. %s"
                  % (fastqc_txt, str(e)))
        finally:
            file_handle.close()


class Status():
    def __init__(self):
        self.Overall = None
        self.PerBaseSequenceQuality = None
        self.PerSequenceQuality = None
        self.PerBaseSequenceContent = None
        self.PerBaseGCContent = None
        self.PerSequenceGCContent = None
        self.PerBaseNContent = None
        self.SequenceLengthDistribution = None
        self.SequenceDuplicationLevels = None
        self.OverrepresentedSequences = None
        self.KMerContent = None


class Sample():
    def __init__(self, name, path):
        self.name = name
        self._path = path
        self._Sequences = None

    def __populate_sequences(self):
        self._Sequences = []

        for directory in os.listdir(self._path):
            try:
                full_dir = os.path.abspath(os.path.join(self._path, directory))
                fastqc_txt = os.path.join(full_dir, "fastqc_data.txt")
                assert(os.path.isfile(fastqc_txt))
                sequence = Sequence()
                sequence.populate(fastqc_txt)
                self._Sequences.append(sequence)
            except AssertionError as incorrect_structure:
                print("Sequence directory has incorrect structure. %s:%s. %s"
                      % (self.name, directory, str(incorrect_structure)))
            except Exception as e:
                print("Unexpected error(s) occurred while processing %s:%s. %s"
                      % (self.name, directory, str(e)))
                raise

    def getSequences(self):
        try:
            assert(self._Sequences is None)
            self.__populate_sequences()
        except AssertionError as sequences_not_empty:
            print("Sample instance of getSequences attempted to overwrite" +
                  " non-empty Sequences attribute: " + str(sequences_not_empty))
        except Exception as e:
            print("Unexpected error(s) occurred in read of Sample: %s, caught" +
                  " exception: %s" % (self.name, str(e)))
            raise

    def sql_statement(self, table_basic, table_stats):
        sql = ""
        values = []

        for seq in self._Sequences:
            sql = sql + "insert into " + table_basic + " (filename, filetype, \
                encoding, total_sequences, filtered_sequences, sequence_length,\
                percentage_gc) values (%s,%s,%s,%s,%s,%s,%s);\ninsert into "\
                + table_stats + " (overall, per_base_sequence_quality, \
                per_sequence_quality_scores, per_base_sequence_content, \
                per_base_gc_content, per_sequence_gc_content, \
                per_base_n_content, sequence_length_distribution, \
                sequence_duplication_levels, overrepresented_sequences, \
                kmer_content) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n"

            values.extend([seq.filename, seq.filetype, seq.encoding,
                           seq.total_seqs, seq.filtered_seqs,
                           seq.sequence_length,
                           seq.GC_Content_Percent, seq.stats.Overall,
                           seq.stats.PerBaseSequenceQuality,
                           seq.stats.PerSequenceQuality,
                           seq.stats.PerBaseSequenceContent,
                           seq.stats.PerBaseGCContent,
                           seq.stats.PerSequenceGCContent,
                           seq.stats.PerBaseNContent,
                           seq.stats.SequenceLengthDistribution,
                           seq.stats.SequenceDuplicationLevels,
                           seq.stats.OverrepresentedSequences,
                           seq.stats.KMerContent])

        return [sql, tuple(values)]


def Read_FastQC(directory):
    Samples = []

    for item in os.listdir(directory):
        if os.path.isdir(os.path.join(directory, item)):
            try:
                path = os.path.join(os.path.abspath(directory), item)
                sample = Sample(item, path)
                sample.getSequences()
                Samples.append(sample)
            except Exception as e:
                print("An error has occurred while processing dir: %s. %s"
                      % (item, str(e)))
    return Samples


class FastQC_Database:
    def __init__(self, database_name, database_user, database_pass, Samples):
        self.database_name = database_name
        self.database_user = database_user
        self.database_pass = database_pass
        self.basic = "fastqc_basic_information"
        self.stats = "fastqc_stats_information"
        self.Samples = Samples
        self.connection = None
        self.cursor = None

    def __connect(self):
        try:
            self.connection = psycopg2.connect(database=self.database_name,
                                               user=self.database_user,
                                               password=self.database_pass)
            self.cursor = self.connection.cursor()
        except Exception as e:
            sys.stdout.write("Exception while connecting: %s\n" % str(e))
            self.__close()

    def __exec_sql(self, sql, values):
        try:
            self.__connect()
            return self.cursor.execute(sql, values)
        except Exception as e:
            sys.stdout.write("Could not execute sql statement:\n%s\n\
                             Raised Exception: %s\n" % (sql, str(e)))
        finally:
            self.__close()

    def __close(self):
        if self.cursor is not None:
            try:
                self.cursor.close()
            except:
                sys.stdout.write("Could not close cursor object\n")

        if self.connection is not None:
            try:
                self.connection.commit()
                self.connection.close()
            except:
                sys.stdout.write("Could not commit, or close the database \
                                 connection object\n")

        self.cursor = None
        self.connection = None

    def Create_Tables(self):
        Basic_Information = "drop table if exists %s;\ncreate table %s (id \
            serial primary key, filename text, filetype text, encoding \
            text, total_sequences text, filtered_sequences text, \
            sequence_length text, percentage_gc smallint);" \
            % (self.basic, self.basic)
        Module_Stats = "drop table if exists %s;\ncreate table %s (id serial \
            primary key, overall text, per_base_sequence_quality text, \
            per_sequence_quality_scores text, per_base_sequence_content \
            text, per_base_gc_content text, per_sequence_gc_content \
            text, per_base_n_content text, sequence_length_distribution \
            text, sequence_duplication_levels text, \
            overrepresented_sequences text, kmer_content text);"\
            % (self.stats, self.stats)
        values = None
        self.__exec_sql(Basic_Information + "\n" + Module_Stats, values)

    def Populate_Database(self):
        for sample in self.Samples:
            sql_values = sample.sql_statement(self.basic, self.stats)
            self.__exec_sql(sql_values[0], sql_values[1])


def main(directory="./fastqc_results", database_name="Fastqc_Results"):
    Samples = Read_FastQC(directory)
    database_name = os.getenv("FASTQC_DATABASE_NAME")
    database_user = os.getenv("FASTQC_DATABASE_USER")
    database_pass = os.getenv("FASTQC_DATABASE_PASS")
    database = FastQC_Database(database_name, database_user, database_pass,
                               Samples)
    database.Create_Tables()
    database.Populate_Database()


if __name__ == "__main__":
    main("./Before_trimming")
