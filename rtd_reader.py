import sys
from sqlalchemy import create_engine
from lxml import etree
import pandas as pd
import arcpy
import numpy as np
import os.path


class RTD(object):
    """A RingToets SQLite database

    <More elaborate doc>
    """

    def __init__(self, rtd_file=None):

        """

        :type rtd_file: path to .rtd file
        """
        self.rtd_file = rtd_file
        if self.rtd_file == None:
            self.rtd_file = 'data/example2.rtd'

        self.engine = create_engine('sqlite:///{}'.format(self.rtd_file))

    def __repr__(self):
        return '{} instance of database "{}"'.format(self.__class__, os.path.abspath(self.rtd_file))

    def list_table_names(self):
        """Return a list of tables in the database
    
        """
        # http://docs.sqlalchemy.org/en/latest/core/connections.html
        with self.engine.connect() as connection:
            result = connection.execute("select name from sqlite_master where type = 'table';").fetchall()
            return [item.name for item in result]

    def export_to_fgdb(self, fgdb):
        """Export all tables to an esri filegeodatabase
    
        """
        if not arcpy.Exists(fgdb):
            arcpy.CreateFileGDB_management(os.path.split(fgdb)[0], os.path.split(fgdb)[1])

        for table in self.list_table_names():
            df = self.table_to_df(table)
            self.df_to_table(df, fgdb + "/" + str(table))

    def table_to_df(self, tbl):
        """SQLite table to pandas dataframe
    
        """
        return pd.read_sql(tbl, self.engine)

    def df_to_table(self, df, out_table):
        """Export a pandas dataframe to a table in a fgdb
    
        """

        # https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table
        # but fails for empty tables
        if len(df.values) > 0:
            x = np.array(np.rec.fromrecords(df.values))
            names = df.dtypes.index.tolist()
            x.dtype.names = tuple([str(name) for name in names])
        else:
            return None

        try:
            arcpy.da.NumPyArrayToTable(x, out_table)
            return out_table

        except RuntimeError:
            e = sys.exc_info()[1]
            print('Failed to write {} because of this error: {}'.format(out_table, e))


class RTD_table(object):
    """A table from a Ringtoets database as a pandas dataframe
    
    """

    def __init__(self, RTD_obj, table_name):
        """
        
        :param RTD_obj: 
        :param table_name: 
        """
        self.RTD = RTD_obj
        self.table_name = table_name

    def __repr__(self):
        return '{} instance of table "{}" in {}'.format(self.__class__,
                                                        self.table_name,
                                                        self.RTD)

    def info(self):
        """Information on shape and contents of the table
        
        :return: 
        """
        info = ""
        info += "{:65} #-Shape: {:10}    #-Bytes: {:_>6}\n".format(self.table_name,
                                                                   str(self.as_df().shape),
                                                                   sys.getsizeof(self.as_df()))

        for col in self.as_df().columns:
            info += "    {:69} #-dtype: {:6}\n".format(col, self.as_df()[col].dtype)

        return info

    def as_df(self):
        """SQLite table as pandas dataframe

        """
        return pd.read_sql(self.table_name, self.RTD.engine)

    def to_fgdb_table(self, fgdb, out_table):
        """Export to a table in a fgdb
        
        :param fgdb: path to filegeodatabase
        :param out_table: name of output table
        :return: path to created table
        """

        #if not hasattr(self,'df'):
        #    self.to_df()

        # https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table
        # but fails for empty tables
        if len(self.as_df().values) > 0:
            x = np.array(np.rec.fromrecords(self.as_df().values))
            names = self.as_df().dtypes.index.tolist()
            x.dtype.names = tuple([str(name) for name in names])
        else:
            return None

        out_path = os.path.join(fgdb, out_table)
        try:
            arcpy.da.NumPyArrayToTable(x, out_path)
            return out_path

        except RuntimeError as e:
            print('Failed to write {} because of this error: {}'.format(out_path, e))

    @property
    def xml_columns(self):
        """Check which columns contain xml

        :return:
        """
        for col in self.as_df().columns:
            if 'xml' in str(col).lower():
                yield col

    def xml_str_to_tree(self, xml_str):
        """Return etree for xml_str
        """

        # http://lxml.de/tutorial.html#the-fromstring-function
        return etree.fromstring(xml_str)

    def get_xyz_from_xml_as_dict(self, tree):
        """Find x, y, z tags in xml and return a dict of their values

        :return:
        """
        data = {'x': [], 'y': [], 'z': []}
        for element in tree.iter():

            if element.tag.split('}')[1] in ('x', 'y', 'z'):
                #tag = element.getparent().tag
                data[element.tag.split("}")[1]].append(float(element.text))

        # Remove empty keys
        for key in data.keys():
            if len(data[key]) == 0:
                data.pop(key, None)
        return data

if __name__ == '__main__':
    rtd = RTD()
    for tbl in rtd.list_table_names():
        t = RTD_table(rtd,tbl)
        # print(t.info())
        for xml_col in t.xml_columns:
            print 'xml_col: {}'.format(xml_col)
            for xml_str in t.as_df()[xml_col]:
                # print(xml_str[:50])
                tree = t.xml_str_to_tree(xml_str)
                print(t.get_xyz_from_xml_as_dict(tree))
