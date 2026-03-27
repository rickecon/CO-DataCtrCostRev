"""
Module for generating property tax layers of Colorado for cost and revenue of
building a data center in Colorado.
"""
# Import packages
from pathlib import Path
import os
import io
import json
import zipfile
import requests
import pickle
import time
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.ops import unary_union

from bokeh.io import output_file, output_notebook, save
from bokeh.plotting import figure, show
from bokeh.models import GeoJSONDataSource, HoverTool, Legend, Title
from bokeh.io import reset_output


def make_co_datactrcostrevmap(
    create_data=False, save_data=True
):
    main_dir = Path(__file__).resolve().parent
    data_dir = os.path.join(main_dir, "data")
    images_dir = os.path.join(main_dir, "images")

    if create_data:
        print("Creating all the data from shapefiles.")
        start_time_all = time.time()
        # ---------------------------------------------------------------------
        # Add Colorado state boundary shape file
        # ---------------------------------------------------------------------
        # Download U.S. states shape files from US Census Bureau
        # https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_state_500k.zip
        print("")
        print("Creating Colorado state boundary shapefile,")
        start_time_co = time.time()
        us_shapefile_path = (
            os.path.join(
                data_dir, "shp", "cb_2023_us_state_500k",
                "cb_2023_us_state_500k.shp"
            )
        )
        states_gdf = gpd.GeoDataFrame.from_file(us_shapefile_path)
        states_gdf_json = states_gdf.to_json()
        states_gjson = json.loads(states_gdf_json)

        # Build a Colorado polygon GeoDataFrame (not GeoJSON) for spatial ops
        co_gdf = states_gdf.loc[states_gdf["STUSPS"] == "CO"].copy()
        # Dissolve in case CO is multipart; makes a single boundary geometry
        co_gdf = co_gdf.dissolve()

        co_gdf_str = co_gdf.to_json()
        co_src = GeoJSONDataSource(geojson=co_gdf_str)

        elapsed_time_co = time.time() - start_time_co
        min = int(elapsed_time_co // 60)
        sec = np.round(elapsed_time_co % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Add Colorado county boundaries shape file
        # ---------------------------------------------------------------------
        print("")
        print("Creating county boundaries shapefile")
        start_time_cnt = time.time()
        county_shapefile_path = os.path.join(
            data_dir, "shp", "cb_2023_us_county_500k",
            "cb_2023_us_county_500k.shp"
        )
        counties_gdf = gpd.GeoDataFrame.from_file(county_shapefile_path)
        # Filter to Colorado counties (STATEFP for Colorado = 08)
        co_counties_gdf = counties_gdf.loc[
            counties_gdf["STATEFP"] == "08"
        ].copy()
        # Match CRS
        co_counties_gdf = co_counties_gdf.to_crs(co_gdf.crs)
        co_counties_gdf_str = co_counties_gdf.to_json()
        # Convert to Bokeh GeoJSON
        co_counties_src = GeoJSONDataSource(geojson=co_counties_gdf_str)

        elapsed_time_cnt = time.time() - start_time_cnt
        min = int(elapsed_time_cnt // 60)
        sec = np.round(elapsed_time_cnt % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create lakes and reservoirs shape file
        # ---------------------------------------------------------------------
        # Natural Earth provides direct download links via their site.
        # https://naturalearth.s3.amazonaws.com/10m_physical/ne_10m_lakes.zip
        print("")
        print("Creating lakes and reservoirs shapefile")
        start_time_lk = time.time()
        lakes_res_shapefile_path = os.path.join(
            data_dir, "shp", "ne_10m_lakes", "ne_10m_lakes.shp"
        )
        lakes_res_gdf = gpd.GeoDataFrame.from_file(lakes_res_shapefile_path)

        # Ensure lakes are in same CRS as Colorado
        lakes_res_gdf = lakes_res_gdf.to_crs(co_gdf.crs)

        # Clip lakes/reservoirs to Colorado boundary
        lakes_res_co_gdf = gpd.clip(lakes_res_gdf, co_gdf)
        lakes_res_co_gdf_str = lakes_res_co_gdf.to_json()
        lakes_res_co_src = GeoJSONDataSource(geojson=lakes_res_co_gdf_str)

        elapsed_time_lk = time.time() - start_time_lk
        min = int(elapsed_time_lk // 60)
        sec = np.round(elapsed_time_lk % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create rivers shape file
        # ---------------------------------------------------------------------
        # Natural Earth provides direct download links via their site.
        # "https://naturalearth.s3.amazonaws.com/10m_physical/" +
        # "ne_10m_rivers_lake_centerlines.zip"
        print("")
        print("Creating rivers shapefile")
        start_time_rv = time.time()
        rivers_lakes_shapefile_path = os.path.join(
            data_dir, "shp",
            "ne_10m_rivers_lake_centerlines",
            "ne_10m_rivers_lake_centerlines.shp"
        )
        rivers_lakes_gdf = gpd.GeoDataFrame.from_file(
            rivers_lakes_shapefile_path
        )

        # Ensure rivers are in same CRS as Florida
        rivers_lakes_gdf = rivers_lakes_gdf.to_crs(co_gdf.crs)
        # Clip rivers to Florida boundary
        rivers_lakes_co_gdf = gpd.clip(rivers_lakes_gdf, co_gdf)
        rivers_lakes_co_gdf_str = rivers_lakes_co_gdf.to_json()
        rivers_lakes_co_src = GeoJSONDataSource(
            geojson=rivers_lakes_co_gdf_str
        )

        elapsed_time_rv = time.time() - start_time_rv
        min = int(elapsed_time_rv // 60)
        sec = np.round(elapsed_time_rv % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado state public access properties (PAP) shape file
        # ---------------------------------------------------------------------
        # https://geodata-cpw.hub.arcgis.com/datasets/f227d7a73ecd4a3cae5e61a83ddd76a9/about
        print("")
        print("Creating state public access properties (PAP) shapefile")
        start_time_pap = time.time()
        state_pap_shapefile_path = os.path.join(
            data_dir, "shp",
            "CPW_PublicAccessProperties",
            "CPWPublicAccessProperties03092026.shp"
        )
        state_pap_gdf = gpd.GeoDataFrame.from_file(
            state_pap_shapefile_path
        )
        # Make sure parks are in the same CRS as Colorado (and therefore the
        # figure)
        state_pap_gdf = state_pap_gdf.to_crs(co_gdf.crs)

        # Clip to Colorado so stray polygons don't expand bounds
        state_pap_co_gdf = gpd.clip(state_pap_gdf, co_gdf)
        state_pap_co_gdf_str = state_pap_co_gdf.to_json()
        state_pap_co_src = GeoJSONDataSource(geojson=state_pap_co_gdf_str)

        elapsed_time_pap = time.time() - start_time_pap
        min = int(elapsed_time_pap // 60)
        sec = np.round(elapsed_time_pap % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado state fee title parcels (FTP) shape file
        # ---------------------------------------------------------------------
        # https://geodata-cpw.hub.arcgis.com/datasets/f227d7a73ecd4a3cae5e61a83ddd76a9/about
        print("")
        print("Creating state fee title parcels (FTP) shapefile")
        start_time_ftp = time.time()
        state_ftp_shapefile_path = os.path.join(
            data_dir, "shp",
            "CPW_PublicAccessProperties",
            "CPWFeeTitleParcels03232026.shp"
        )
        state_ftp_gdf = gpd.GeoDataFrame.from_file(
            state_ftp_shapefile_path
        )
        # Make sure parks are in the same CRS as Colorado (and therefore the
        # figure)
        state_ftp_gdf = state_ftp_gdf.to_crs(co_gdf.crs)

        # Clip to Colorado so stray polygons don't expand bounds
        state_ftp_co_gdf = gpd.clip(state_ftp_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = state_ftp_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            state_ftp_co_gdf[c] = state_ftp_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        state_ftp_co_gdf_str = state_ftp_co_gdf.to_json()
        state_ftp_co_src = GeoJSONDataSource(geojson=state_ftp_co_gdf_str)

        elapsed_time_ftp = time.time() - start_time_ftp
        min = int(elapsed_time_ftp // 60)
        sec = np.round(elapsed_time_ftp % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Create Colorado national parks shape file
        # # ---------------------------------------------------------------------
        # # https://mapdirect-fdep.opendata.arcgis.com/datasets/national-park-boundaries
        # print("")
        # print("Creating national parks shapefile")
        # start_time_np = time.time()
        # nat_parks_shapefile_path = os.path.join(
        #     data_dir, "shp",
        #     "National_Park_Boundaries",
        #     "National_Park_Boundaries.shp"
        # )
        # nat_parks_gdf = gpd.GeoDataFrame.from_file(nat_parks_shapefile_path)
        # # Make sure parks are in the same CRS as Colorado (and therefore the
        # # figure)
        # nat_parks_gdf = nat_parks_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # nat_parks_co_gdf = gpd.clip(nat_parks_gdf, co_gdf)

        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = nat_parks_co_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     nat_parks_co_gdf[c] = nat_parks_co_gdf[c].dt.strftime(
        #         "%Y-%m-%dT%H:%M:%S"
        #     )
        # nat_parks_co_gdf_str = nat_parks_co_gdf.to_json()
        # nat_parks_co_src = GeoJSONDataSource(geojson=nat_parks_co_gdf_str)

        # elapsed_time_np = time.time() - start_time_np
        # min = int(elapsed_time_np // 60)
        # sec = np.round(elapsed_time_np % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Add Florida public, charter, and nonprofit private schools from
        # # National School Lunch Program (NSLP) 2019 data
        # # ---------------------------------------------------------------------
        # print("")
        # print("Creating NSLP schools shapefile")
        # start_time_nlsp = time.time()
        # nlsp_shapefile_path = os.path.join(
        #     data_dir, "shp", "NSLP_Sites_2019", "NSLP_Sites_2019.shp"
        # )

        # nlsp_gdf = gpd.GeoDataFrame.from_file(nlsp_shapefile_path)
        # # Make sure parks are in the same CRS as Florida (and therefore the
        # # figure)
        # nlsp_gdf = nlsp_gdf.to_crs(fl_gdf.crs)
        # # Clip to Florida so stray polygons don't expand bounds
        # nlsp_gdf = gpd.clip(nlsp_gdf, fl_gdf)

        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = nlsp_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     nlsp_gdf[c] = nlsp_gdf[c].dt.strftime("%Y-%m-%dT%H:%M:%S")
        # nlsp_gdf_str = nlsp_gdf.to_json()
        # nlsp_src = GeoJSONDataSource(geojson=nlsp_gdf_str)

        # elapsed_time_nlsp = time.time() - start_time_nlsp
        # min = int(elapsed_time_nlsp // 60)
        # sec = np.round(elapsed_time_nlsp % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Add Florida public k-12 schools from NCES data
        # # ---------------------------------------------------------------------
        # print("")
        # print("Creating public schools shapefile")
        # start_time_psch = time.time()
        # pub_schl_shapefile_path = os.path.join(
        #     data_dir, "shp",
        #     "EDGE_GEOCODE_PUBLICSCH_2425",
        #     "Shapefile_SCH",
        #     "EDGE_GEOCODE_PUBLICSCH_2425.shp"
        # )

        # pub_schl_gdf = gpd.GeoDataFrame.from_file(pub_schl_shapefile_path)
        # # Make sure schools are in the same CRS as Florida (and therefore the
        # # figure)
        # pub_schl_gdf = pub_schl_gdf.to_crs(fl_gdf.crs)
        # # Clip to Florida so stray polygons don't expand bounds
        # pub_schl_gdf = gpd.clip(pub_schl_gdf, fl_gdf)

        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = pub_schl_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     pub_schl_gdf[c] = pub_schl_gdf[c].dt.strftime("%Y-%m-%dT%H:%M:%S")
        # pub_schl_gdf_str = pub_schl_gdf.to_json()
        # pub_schl_src = GeoJSONDataSource(geojson=pub_schl_gdf_str)

        # elapsed_time_psch = time.time() - start_time_psch
        # min = int(elapsed_time_psch // 60)
        # sec = np.round(elapsed_time_psch % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Add Florida public postsecondary schools from NCES data
        # # ---------------------------------------------------------------------
        # print("")
        # print("Creating public postsecondary schools shapefile")
        # start_time_pssch = time.time()
        # pub_pstsec_schl_shapefile_path = os.path.join(
        #     data_dir, "shp",
        #     "EDGE_GEOCODE_POSTSECSCH_2425",
        #     "SHAPEFILE",
        #     "EDGE_GEOCODE_POSTSECSCH_2425.shp"
        # )

        # pub_pstsec_schl_gdf = gpd.GeoDataFrame.from_file(
        #     pub_pstsec_schl_shapefile_path
        # )
        # # Make sure parks are in the same CRS as Florida (and therefore the
        # # figure)
        # pub_pstsec_schl_gdf = pub_pstsec_schl_gdf.to_crs(fl_gdf.crs)
        # # Clip to Florida so stray polygons don't expand bounds
        # pub_pstsec_schl_gdf = gpd.clip(pub_pstsec_schl_gdf, fl_gdf)

        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = pub_pstsec_schl_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     pub_pstsec_schl_gdf[c] = pub_pstsec_schl_gdf[c].dt.strftime("%Y-%m-%dT%H:%M:%S")
        # pub_pstsec_schl_gdf_str = pub_pstsec_schl_gdf.to_json()
        # pub_pstsec_schl_src = GeoJSONDataSource(
        #     geojson=pub_pstsec_schl_gdf_str
        # )

        # elapsed_time_pssch = time.time() - start_time_pssch
        # min = int(elapsed_time_pssch // 60)
        # sec = np.round(elapsed_time_pssch % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Add Florida private schools shapefile from NCES data
        # # ---------------------------------------------------------------------
        # print("")
        # print("Creating private schools shapefile")
        # start_time_pvsch = time.time()
        # priv_schl_shapefile_path = os.path.join(
        #     data_dir, "shp",
        #     "EDGE_GEOCODE_PRIVATESCH_2324", "EDGE_GEOCODE_PRIVATESCH_2324.shp"
        # )

        # priv_schl_gdf = gpd.GeoDataFrame.from_file(priv_schl_shapefile_path)
        # # Make sure parks are in the same CRS as Florida (and therefore the
        # # figure)
        # priv_schl_gdf = priv_schl_gdf.to_crs(fl_gdf.crs)
        # # Clip to Florida so stray polygons don't expand bounds
        # priv_schl_gdf = gpd.clip(priv_schl_gdf, fl_gdf)

        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = priv_schl_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     priv_schl_gdf[c] = priv_schl_gdf[c].dt.strftime(
        #         "%Y-%m-%dT%H:%M:%S"
        #     )
        # priv_schl_gdf_str = priv_schl_gdf.to_json()
        # priv_schl_src = GeoJSONDataSource(geojson=priv_schl_gdf_str)

        # elapsed_time_pvsch = time.time() - start_time_pvsch
        # min = int(elapsed_time_pvsch // 60)
        # sec = np.round(elapsed_time_pvsch % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Add Florida residences and businesses shapefile from NCES data. This
        # # file is the biggest of all and takes 40 minutes to read in and
        # # process.
        # # ---------------------------------------------------------------------
        # print("")
        # print("Creating residences and businesses land shapefile")
        # start_time_resbus = time.time()
        # res_bus_shapefile_path = os.path.join(
        #     data_dir, "shp", "FL_res_bus", "Florida.geojson"
        # )

        # res_bus_gdf = gpd.GeoDataFrame.from_file(res_bus_shapefile_path)
        # # Make sure parks are in the same CRS as Florida (and therefore the
        # # figure)
        # res_bus_gdf = res_bus_gdf.to_crs(fl_gdf.crs)
        # # Clip to Florida so stray polygons don't expand bounds
        # res_bus_gdf = gpd.clip(res_bus_gdf, fl_gdf)

        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = res_bus_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     res_bus_gdf[c] = res_bus_gdf[c].dt.strftime("%Y-%m-%dT%H:%M:%S")
        # res_bus_gdf_str = res_bus_gdf.to_json()
        # res_bus_src = GeoJSONDataSource(geojson=res_bus_gdf_str)

        # elapsed_time_resbus = time.time() - start_time_resbus
        # min = int(elapsed_time_resbus // 60)
        # sec = np.round(elapsed_time_resbus % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Create random sample data of 200k Florida residences and businesses
        # # from res_bus_gdf
        # # ---------------------------------------------------------------------
        # print("")
        # print(
        #     "Creating sample residences and businesses land GeoPandasDataFrame"
        # )
        # sample_size = 200000
        # start_time_resbussamp = time.time()
        # res_bus_shapefile_path = os.path.join(
        #     data_dir, "shp", "FL_res_bus", "Florida.geojson"
        # )
        # # Use projected CRS for centroid calculation
        # res_bus_proj = res_bus_gdf.to_crs("EPSG:3086")
        # # Keep attributes, replace polygon geometry with points
        # res_bus_pts_gdf = res_bus_proj.copy()
        # # Sample the points
        # res_bus_pts_gdf = res_bus_pts_gdf.sample(sample_size, random_state=42)
        # res_bus_pts_gdf[
        #     "geometry"
        # ] = res_bus_proj.geometry.representative_point()
        # # Convert back to plotting CRS
        # res_bus_pts_gdf = res_bus_pts_gdf.to_crs(fl_gdf.crs)
        # # Find datetime-ish columns and convert to ISO strings
        # dt_cols = res_bus_pts_gdf.select_dtypes(
        #     include=["datetime64[ns]", "datetime64[ns, UTC]"]
        # ).columns
        # for c in dt_cols:
        #     res_bus_pts_gdf[c] = res_bus_pts_gdf[c].dt.strftime(
        #         "%Y-%m-%dT%H:%M:%S"
        #     )
        # # Optional: keep only columns you actually need
        # # This matters a lot for file size and plotting speed
        # cols_to_keep = ["geometry"]
        # # for col in ["county", "house_district", "senate_district"]:
        # #     if col in res_bus_pts_gdf.columns:
        # #         cols_to_keep.append(col)
        # res_bus_pts_gdf = res_bus_pts_gdf[cols_to_keep].copy()
        # res_bus_pts_gdf_str = res_bus_pts_gdf.to_json()
        # res_bus_pts_src = GeoJSONDataSource(geojson=res_bus_pts_gdf_str)

        # elapsed_time_resbussamp = time.time() - start_time_resbussamp
        # min = int(elapsed_time_resbussamp // 60)
        # sec = np.round(elapsed_time_resbussamp % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # # ---------------------------------------------------------------------
        # # Add Florida parcels shapefile (see Florida Geospatial Open Data
        # # Portal)
        # # ---------------------------------------------------------------------
        # # "https://geodata.floridagio.gov/datasets/" +
        # # "efa909d6b1c841d298b0a649e7f71cf2_0/" +
        # # "explore?location=0.008995%2C0.000000%2C1.00"

        # # ---------------------------------------------------------------------
        # # Create available area shape file. This process takes 8 minutes.
        # # ---------------------------------------------------------------------
        # print("")
        # print("Creating available land shapefile")
        # start_time_avl = time.time()
        # five_miles_in_meters = 8046.72

        # # Load datasets (examples: replace with your actual file paths)
        # bnd_fl = fl_gdf.copy()  # Also have fl_geojson
        # bnd_lakes_res = lakes_res_gdf.copy()
        # bnd_rivers_lakes = rivers_lakes_gdf.copy()
        # bnd_state_parks = state_parks_gdf.copy()
        # bnd_pub_schl = pub_schl_gdf.copy()
        # bnd_pub_pstsec_schl = pub_pstsec_schl_gdf.copy()
        # bnd_priv_schl = priv_schl_gdf.copy()
        # bnd_nat_parks = nat_parks_gdf.copy()
        # bnd_res_bus = res_bus_gdf.copy()

        # # Reproject everything to a meters-based CRS (statewide)
        # target_crs = "EPSG:3086"
        # layers_lst = [
        #     bnd_fl, bnd_lakes_res, bnd_rivers_lakes, bnd_state_parks,
        #     bnd_pub_schl, bnd_pub_pstsec_schl, bnd_priv_schl, bnd_nat_parks,
        #     bnd_res_bus
        # ]
        # layers_lst = [gdf.to_crs(target_crs) for gdf in layers_lst]
        # (
        #     bnd_fl_crs, bnd_lakes_res_crs, bnd_rivers_lakes_crs,
        #     bnd_state_parks_crs, bnd_pub_schl_crs, bnd_pub_pstsec_schl_crs,
        #     bnd_priv_schl_crs, bnd_nat_parks_crs, bnd_res_bus_crs
        # ) = layers_lst

        # # Buffer each layer by either 1 meter or 5 miles
        # buf_lakes_res = bnd_lakes_res_crs.buffer(1)
        # buf_rivers_lakes = bnd_rivers_lakes_crs.buffer(1)
        # buf_state_parks = bnd_state_parks_crs.buffer(1)
        # buf_nat_parks = bnd_nat_parks_crs.buffer(1)
        # buf_pub_schl = bnd_pub_schl_crs.buffer(five_miles_in_meters)
        # buf_pub_pstsec_schl = bnd_pub_pstsec_schl_crs.buffer(
        #     five_miles_in_meters
        # )
        # buf_priv_schl = bnd_priv_schl_crs.buffer(five_miles_in_meters)
        # buf_res_bus = bnd_res_bus_crs.buffer(five_miles_in_meters)

        # # Union all exclusion buffers (this is the expensive part)
        # exclusion_geom = unary_union(
        #     list(buf_lakes_res) + list(buf_rivers_lakes) +
        #     list(buf_state_parks) + list(buf_nat_parks) +
        #     list(buf_pub_schl) + list(buf_pub_pstsec_schl) +
        #     list(buf_priv_schl) + list(buf_res_bus)
        # )

        # exclusion_gdf = gpd.GeoDataFrame(
        #     geometry=[exclusion_geom], crs=target_crs
        # )

        # # Compute available land
        # # (If your Florida boundary includes offshore waters, use a "land-only"
        # # polygon if possible.)
        # available_geom = bnd_fl_crs.geometry.iloc[0].difference(exclusion_geom)
        # available_gdf = gpd.GeoDataFrame(
        #     geometry=[available_geom], crs=target_crs
        # )

        # # Make CRSs match (important)
        # if available_gdf.crs is None:
        #     raise ValueError(
        #         "available_gdf has no CRS; set it before plotting."
        #     )
        # if available_gdf.crs != fl_gdf.crs:
        #     # Reproject available CRS (or vice versa)
        #     available_gdf = available_gdf.to_crs(fl_gdf.crs)

        # # Convert to GeoJSON sources
        # # fl_src = GeoJSONDataSource(geojson=fl.to_json())
        # available_gdf_str = available_gdf.to_json()
        # available_src = GeoJSONDataSource(geojson=available_gdf_str)

        # # Bounds from Florida
        # minx, miny, maxx, maxy = fl_gdf.total_bounds
        # padx = (maxx - minx) * 0.03
        # pady = (maxy - miny) * 0.03

        # elapsed_time_avl = time.time() - start_time_avl
        # min = int(elapsed_time_avl // 60)
        # sec = np.round(elapsed_time_avl % 60, 1)
        # print(f"took {min} minutes and {sec} seconds.")

        # Create dictionaries of GeoDataFrames and GeoJSONDataSources for all
        # layers
        gdf_dict = {
            "co_gdf": co_gdf,
            "co_counties_gdf": co_counties_gdf,
            "lakes_res_co_gdf": lakes_res_co_gdf,
            "rivers_lakes_co_gdf": rivers_lakes_co_gdf,
            "state_pap_co_gdf": state_pap_co_gdf,
            "state_ftp_co_gdf": state_ftp_co_gdf
            # "nat_parks_co_gdf": nat_parks_co_gdf
            # "nlsp_gdf": nlsp_gdf,
            # "pub_schl_gdf": pub_schl_gdf,
            # "pub_pstsec_schl_gdf": pub_pstsec_schl_gdf,
            # "priv_schl_gdf": priv_schl_gdf,
            # "res_bus_gdf": res_bus_gdf,
            # "res_bus_pts_gdf": res_bus_pts_gdf,
            # "available_gdf": available_gdf
        }
        geojson_dict = {
            "co_gdf_str": co_gdf_str,
            "co_counties_gdf_str": co_counties_gdf_str,
            "lakes_res_co_gdf_str": lakes_res_co_gdf_str,
            "rivers_lakes_co_gdf_str": rivers_lakes_co_gdf_str,
            "state_pap_co_gdf_str": state_pap_co_gdf_str,
            "state_ftp_co_gdf_str": state_ftp_co_gdf_str
            # "nat_parks_co_gdf_str": nat_parks_co_gdf_str
            # "nlsp_gdf_str": nlsp_gdf_str,
            # "pub_schl_gdf_str": pub_schl_gdf_str,
            # "pub_pstsec_schl_gdf_str": pub_pstsec_schl_gdf_str,
            # "priv_schl_gdf_str": priv_schl_gdf_str,
            # "res_bus_gdf_str": res_bus_gdf_str,
            # "res_bus_pts_gdf_str": res_bus_pts_gdf_str,
            # "available_gdf_str": available_gdf_str
        }
        src_dict = {
            "co_src": co_src,
            "co_counties_src": co_counties_src,
            "lakes_res_co_src": lakes_res_co_src,
            "rivers_lakes_co_src": rivers_lakes_co_src,
            "state_pap_co_src": state_pap_co_src,
            "state_ftp_co_src": state_ftp_co_src
            # "nat_parks_co_src": nat_parks_co_src,
            # "nlsp_src": nlsp_src,
            # "pub_schl_src": pub_schl_src,
            # "pub_pstsec_schl_src": pub_pstsec_schl_src,
            # "priv_schl_src": priv_schl_src,
            # "res_bus_src": res_bus_src,
            # "res_bus_pts_src": res_bus_pts_src,
            # "available_src": available_src
        }
        if save_data:
            for name, gdf in gdf_dict.items():
                pickle.dump(
                    gdf, open(
                        os.path.join(data_dir, "gdf", f"{name}.pkl"), "wb"
                    )
                )
            for name, geojson in geojson_dict.items():
                with open(
                    os.path.join(data_dir, "geojson", f"{name}.geojson"),
                    "w", encoding="utf-8"
                ) as f:
                    f.write(geojson)

        elapsed_time_all = time.time() - start_time_all
        min = int(elapsed_time_all // 60)
        sec = np.round(elapsed_time_all % 60, 1)
        print("")
        print(f"Total data creation took {min} minutes and {sec} seconds.")
    else:
        print("")
        print("Reading in all the data from hard drive,")
        start_time = time.time()

        gdf_name_list = [
            "co_gdf",
            "co_counties_gdf",
            "lakes_res_co_gdf",
            "rivers_lakes_co_gdf",
            "state_pap_co_gdf",
            "state_ftp_co_gdf"
            # "nat_parks_co_gdf"
            # "nlsp_gdf",
            # "pub_schl_gdf",
            # "pub_pstsec_schl_gdf",
            # "priv_schl_gdf",
            # "res_bus_gdf",
            # "res_bus_pts_gdf",
            # "available_gdf"
        ]
        gdf_dict = {
            os.name: pickle.load(
                open(os.path.join(data_dir, "gdf", f"{name}.pkl"), "rb")
            ) for name in gdf_name_list
        }

        geojson_name_list = [
            "co_gdf_str",
            "co_counties_gdf_str",
            "lakes_res_co_gdf_str",
            "rivers_lakes_co_gdf_str",
            "state_pap_co_gdf_str",
            "state_ftp_co_gdf_str"
            # "nat_parks_co_gdf_str"
            # "nlsp_gdf_str",
            # "pub_schl_gdf_str",
            # "pub_pstsec_schl_gdf_str",
            # "priv_schl_gdf_str",
            # "res_bus_gdf_str",
            # "res_bus_pts_gdf_str",
            # "available_gdf_str"
        ]
        src_dict = {}
        for name in geojson_name_list:
            path = os.path.join(data_dir, "geojson", f"{name}.geojson")
            with open(path, "r", encoding="utf-8") as f:
                obj_str = f.read()
            obj_src = GeoJSONDataSource(geojson=obj_str)
            src_name = name.split("_gdf_str")[0] + "_src"
            src_dict[src_name] = obj_src

        elapsed_time = time.time() - start_time
        min = int(elapsed_time // 60)
        sec = np.round(elapsed_time % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

    # -------------------------------------------------------------------------
    # Make figure
    # -------------------------------------------------------------------------
    fig1_title = (
        "Figure 1. Colorado map of data center cost and revenue"
    )

    # fig1_title = ""
    fig1_filename = "co_datactrcostrevmap.html"
    output_file(
        "./images/" + fig1_filename, title=fig1_title, mode='inline'
    )

    TOOLS = "pan, box_zoom, wheel_zoom, hover, save, reset, help"

    fig1 = figure(
        title=fig1_title,
        height=700,
        width=1180,
        tools=TOOLS,
        min_border = 0,
        x_axis_location = None, y_axis_location = None,
        toolbar_location="right"
    )
    fig1.toolbar.logo = None
    fig1.grid.grid_line_color = None

    # Colorado state outline
    print("Fig 1 Status: Plotting co_src")
    r_co = fig1.patches(
        "xs", "ys",
        source=src_dict["co_src"],
        fill_alpha=0.00,
        line_color="black",
        line_width=2,
        fill_color="white"
    )

    # Colorado counties outline
    print("Fig 1 Status: Plotting co_counties_src")
    r_counties = fig1.patches(
        "xs", "ys",
        source=src_dict["co_counties_src"],
        fill_alpha=0.00,
        line_color="black",
        line_width=1,
        muted_alpha=0.04
    )

    # Lakes / reservoirs
    print("Fig 1 Status: Plotting lakes_res_co_src")
    r_lakes = fig1.patches(
        "xs", "ys",
        source=src_dict["lakes_res_co_src"],
        fill_color="blue",
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.5,
        muted_alpha=0.0
    )

    # Rivers / Lakes
    print("Fig 1 Status: Plotting rivers_lakes_src")
    r_rivers = fig1.multi_line(
        "xs", "ys",
        source=src_dict["rivers_lakes_co_src"],
        line_color="blue",
        line_alpha=0.8,
        line_width=0.8,
        muted_alpha=0.0
    )

    # Colorado state public access properties boundaries
    print("Fig 1 Status: Plotting state_pap_co_src")
    r_state_pap = fig1.patches(
        "xs", "ys",
        source=src_dict["state_pap_co_src"],
        fill_color="brown",
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado state fee title parcels boundaries
    print("Fig 1 Status: Plotting state_ftp_co_src")
    r_state_ftp = fig1.patches(
        "xs", "ys",
        source=src_dict["state_ftp_co_src"],
        fill_color="rosybrown",
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # # Colorado national park boundaries
    # print("Fig 1 Status: Plotting nat_parks_co_src")
    # r_nat_parks = fig1.patches(
    #     "xs", "ys",
    #     source=src_dict["nat_parks_co_src"],
    #     fill_color="saddlebrown",
    #     fill_alpha=0.4,
    #     line_alpha=0.8,
    #     line_width=0.2,
    #     muted_alpha=0.0
    # )

    # # Florida public schools k-12 from NCES data
    # # Create scatter plot of pub_schl_src data
    # print("Fig 1 Status: Plotting pub_schl_src")
    # r_pub_schl = fig1.scatter(
    #     "x", "y",
    #     source=src_dict["pub_schl_src"],
    #     color="gold",
    #     fill_alpha=0.6,
    #     line_alpha=0.1,
    #     size=1,
    #     muted_alpha=0.0
    # )

    # # Florida public postsecondary schools from NCES data
    # # Create scatter plot of pub_pstsec_schl_src data
    # print("Fig 1 Status: Plotting pub_pstsec_schl_src")
    # r_pub_pstsec_schl = fig1.scatter(
    #     "x", "y",
    #     source=src_dict["pub_pstsec_schl_src"],
    #     color="orange",
    #     fill_alpha=0.6,
    #     line_alpha=0.1,
    #     size=1,
    #     muted_alpha=0.0
    # )

    # # Florida private schools from NCES data
    # # Create scatter plot of priv_schl_src data
    # print("Fig 1 Status: Plotting priv_schl_src")
    # r_priv_schl = fig1.scatter(
    #     "x", "y",
    #     source=src_dict["priv_schl_src"],
    #     color="coral",
    #     fill_alpha=0.6,
    #     line_alpha=0.1,
    #     size=1,
    #     muted_alpha=0.0
    # )

    # # Florida residence and buildings sample of 200,000 centroids or
    # # representative pointse +7 million polygons. Don't plot the +7 million
    # # polygons because it will break the computation and browser rendering.
    # r_res_bus_pts = fig1.scatter(
    #     "x", "y",
    #     source=src_dict["res_bus_pts_src"],
    #     color="red",
    #     fill_alpha=0.6,
    #     line_alpha=0.1,
    #     size=1,
    #     muted_alpha=0.0
    # )

    # # Available area overlay
    # print("Fig 1 Status: Plotting avail_src")
    # r_available = fig1.patches(
    #     "xs", "ys",
    #     source=src_dict["available_src"],
    #     fill_color="green",
    #     fill_alpha=1.0,
    #     line_alpha=0.6,
    #     line_width=0.2,
    #     muted_alpha=0.0
    # )

    fig1_legend = Legend(items=[
        # ("Available data center land", [r_available]),
        ("Colorado counties", [r_counties]),
        ("Lakes and reservoirs", [r_lakes]),
        ("Rivers", [r_rivers]),
        ("State Public Access Properties", [r_state_pap]),
        ("State Fee Title Parcels", [r_state_ftp]),
        # ("National Parks", [r_nat_parks])
        # ("Public schools (K-12)", [r_pub_schl]),
        # ("Public schools (post sec.)", [r_pub_pstsec_schl]),
        # ("Private schools", [r_priv_schl]),
        # ("Residences and businesses (sample)", [r_res_bus_pts])
    ])
    fig1.add_layout(fig1_legend)

    # Legend properties
    fig1.legend.click_policy="mute"
    # fig1.legend.location="right"
    fig1.add_layout(fig1.legend[0], 'right')
    fig1.legend.orientation="vertical"
    fig1.legend.background_fill_color="white"
    fig1.legend.background_fill_alpha=0.9
    fig1.legend.border_line_color="black"
    fig1.legend.border_line_width=1
    fig1.legend.label_text_font_size="10pt"
    fig1.legend.spacing=2
    fig1.legend.padding=6
    fig1.legend.margin=6

    # # Set up hover tool
    hover = fig1.select_one(HoverTool)
    hover.point_policy = "follow_mouse"
    hover.tooltips = [
        ("County", "@county")
    ]
    # hover_county = HoverTool(
    #     tooltips=[("County", "@county")],
    #     point_policy="follow_mouse"
    # )
    # hover_county.renderers = [r_counties]
    # fig1.add_tools(hover_county)

    # note_text_list1 = [
    #     (
    #         '  Source: Richard W. Evans (@RickEcon), updated Mar. 9, 2026. ' +
    #         'Note that the business and residence'
    #     ),
    #     (
    #         '      scatter points are a random sample of 200,000 centroid ' +
    #         'points from the over 7 million residences and'
    #     ),
    #     (
    #         '      businesses in the data. I used the full sample of ' +
    #         'business and residence outlines in the calculation for'
    #     ),
    #     (
    #         '      available data center land, but I excluded the majority ' +
    #         'of those points from this figure in order to'
    #     ),
    #     ('      successfully plot them.')
    # ]
    # for note_text in note_text_list1:
    #     caption1 = Title(
    #         text=note_text, align='left', text_font_size='9pt',
    #         text_font_style='italic',
    #         text_color='black',
    #         standoff=0
    #     )
    #     fig1.add_layout(caption1, 'below')

    show(fig1)
