import sys
import os.path
from sqlalchemy import create_engine
from lxml import etree
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon

class RTD(object):
    """A RingToets SQLite database

    <More elaborate doc>
    """

    def __init__(self, rtd_file):

        """

        :type rtd_file: path to .rtd file
        """
        self.rtd_file = rtd_file
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

    def table_info(self, table_name):
        """Information on shape and contents of the table

        :return:
        """
        # TODO: check if using pandas info method is a better solution
        df = self.table_to_df(table_name)
        info = ""
        info += "{:65} #-Shape: {:10}    #-Bytes: {:_>6}\n".format(table_name,
                                                                   str(df.shape),
                                                                   sys.getsizeof(df))

        for col in df.columns:
            info += "    {:69} #-dtype: {:6}\n".format(col, df[col].dtype)

        return info

    def list_columns(self, table_name):
        """Return a list of columns in a table

        """
        with self.engine.connect() as connection:
            result = connection.execute('SELECT * FROM {}'.format(table_name))
            return result.keys()

    def list_geo_xml_columns(self, table_name):
        """List all columns in a table that contain geometry in xml

        """
        return [col for col in self.list_columns(table_name) if col.lower().endswith('xml')]

    def table_to_df(self, tbl):
        """SQLite table to pandas dataframe
    
        """
        return pd.read_sql('SELECT * FROM {}'.format(tbl), self.engine)

    def table_to_geodf(self, table_name, geotype='point'):
        """Return a table as geopandas GeoDataframe

        """
        gdf = gpd.GeoDataFrame(self.table_to_df(table_name))

        for col in self.list_geo_xml_columns(table_name):
            geo_column_name = col +'_geo'
            gdf[geo_column_name] = gdf[col].apply(xml_to_geometry, geotype=geotype)

        gdf.set_geometry(geo_column_name, inplace=True)

        return gdf

def xml_str_to_tree(xml_str):
    """Return etree for xml_str
    """

    return etree.fromstring(xml_str)


def get_xyz_from_xml_as_dict(tree):
    """Find x, y, z tags in xml and return a dict of their values

    :return:
    """
    data = {'x': [], 'y': [], 'z': []}
    for element in tree.iter():

        if element.tag.split('}')[1] in ('x', 'y', 'z'):
            # tag = element.getparent().tag
            data[element.tag.split("}")[1]].append(float(element.text))

    # Remove empty keys
    for key in data.keys():
        if len(data[key]) == 0:
            data.pop(key, None)
    return data


def xml_to_geometry(xml_str, geotype=None):
    """Turn xml string into shapely geometry

    """

    tree = xml_str_to_tree(xml_str)
    xyz_dict = get_xyz_from_xml_as_dict(tree)
    coords = zip(xyz_dict['x'], xyz_dict['y'])
    # TODO: determine geometrytype (point, line, polygon) and act accordingly
    # TODO: z values

    if geotype == 'line':
        geometry = LineString(coords)
    elif geotype == 'point':
        geometry = MultiPoint(coords)
    elif geotype == 'polygon':
        geometry = Polygon(coords)
    else:
        return None

    return geometry


if __name__ == '__main__':
   pass
