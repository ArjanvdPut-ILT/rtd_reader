import sys
import os.path
from sqlalchemy import create_engine
from lxml import etree
import pandas as pd
import geopandas as gpd
import fiona
from shapely.geometry import MultiPoint, LineString, Polygon, Point


class RTD(object):
    """A RingToets SQLite database

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
        df = self.table_to_df(table_name)
        info = ""
        info += "{:65} #-Shape: {:10}    #-Bytes: {:_>6}\n".format(table_name,
                                                                   str(df.shape),
                                                                   sys.getsizeof(df))

        for col in df.columns:
            info += "    {:69} #-dtype: {:6}\n".format(col, str(df[col].dtype))
            if col in self.list_geo_xml_columns(table_name):
                info += "    --> xml geom type = {}\n".format(self.xml_geom_type(table_name, col))

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

    def xml_geom_type(self, table_name, column_name):
        """Return geometry type for column_name in table_name

        """
        df = self.table_to_df(table_name)

        if df.shape[0] == 0:
            return 'No records to determine geometry type'

        return get_xml_geom_type(xml_str_to_tree(df[column_name][0]))

    def table_to_df(self, tbl):
        """SQLite table to pandas dataframe
    
        """
        return pd.read_sql('SELECT * FROM {}'.format(tbl), self.engine)

    def table_to_geodf(self, table_name, column_name, geotype):
        """Return a table as geopandas GeoDataframe

        """
        gdf = gpd.GeoDataFrame(self.table_to_df(table_name))

        geo_column_name = column_name + '_geo'
        gdf[geo_column_name] = gdf[column_name].apply(xml_to_geometry, geotype=geotype)

        # Set geometry to last added column
        #gdf.set_geometry(geo_column_name, inplace=True)

        # Set RD_New as CRS
        gdf.crs = fiona.crs.from_epsg(28992)

        return gdf.set_geometry(geo_column_name)


def xml_str_to_tree(xml_str):
    """Return etree for xml_str

    """

    return etree.fromstring(xml_str)

def get_xml_geom_type(tree):
    """Get xml geometry type from xml tree

    """
    return tree.tag.split('}')[1]


def get_xyz_from_xml_as_dict(tree):
    """Find x, y, z tags in xml and return a dict of their values

    :return:
    """
    xml_geom_type = tree.tag.split('}')[1]

    if xml_geom_type == 'ArrayOfPoint3DXmlSerializer.SerializablePoint3D':
        data = get_tags_from_xml_tree(tree, tags=('x','y','z'))

    elif xml_geom_type == 'ArrayOfPoint2DXmlSerializer.SerializablePoint2D':
        data = get_tags_from_xml_tree(tree, tags=('x', 'y'))

    elif xml_geom_type == 'ArrayOfRoughnessPointXmlSerializer.SerializableRoughnessPoint':
        data = get_tags_from_xml_tree(tree, tags=('x', 'y', 'roughness'))

    else:
        data = get_tags_from_xml_tree(tree, tags=('x', 'y'))

    return data


def get_tags_from_xml_tree(tree, tags=('x','y')):
    """Extract given tags from xml

    """
    data = {tag: [] for tag in tags}

    for element in tree.iter():

        if element.tag.split('}')[1] in tags:
            data[element.tag.split("}")[1]].append(float(element.text))

    return data


def coords_from_xml(xml_str):
    """Return a list of coordinates extracted from xml_string

    """
    tree = xml_str_to_tree(xml_str)
    xyz_dict = get_xyz_from_xml_as_dict(tree)

    return list(zip(*(xyz_dict[key] for key in sorted(xyz_dict.keys()))))


def xml_to_geometry(xml_str, geotype):
    """Turn xml string into shapely geometry

    geotype::   'point'|'line'|'multipoint'|'polygon'
    """
    coords = coords_from_xml(xml_str)

    if geotype == 'line':
        geometry = LineString(coords)
    elif geotype == 'point':
        geometry = MultiPoint(coords)
    elif geotype == 'multipoint':
        geometry = MultiPoint(coords)
    elif geotype == 'polygon':
        geometry = Polygon(coords)
    else:
        return None

    return geometry


if __name__ == '__main__':
    pass
