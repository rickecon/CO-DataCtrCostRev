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

from bokeh.io import output_file
from bokeh.plotting import figure, show
from bokeh.palettes import Viridis256
from bokeh.models import GeoJSONDataSource, HoverTool, Legend


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
        # "https://www2.census.gov/geo/tiger/GENZ2023/shp/" +
        # "cb_2023_us_state_500k.zip"
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
        # Create lake, reservoirs, and rivers shape file
        # ---------------------------------------------------------------------
        # Source: USGS National Hydrography Dataset (NHD) best resolution
        # https://apps.nationalmap.gov/downloader/#/
        print("")
        print("Creating lakes and reservoirs shapefile")
        start_time_lk = time.time()
        lakes_res_riv_shapefile_path = os.path.join(
            data_dir, "shp",
            "NHD_H_Colorado_State_Shape",
            "Shape",
            "NHDArea.shp"
        )
        lakes_res_riv_gdf = gpd.GeoDataFrame.from_file(
            lakes_res_riv_shapefile_path
        )

        # Ensure lakes, reservoirs, and rivers are in same CRS as Colorado
        lakes_res_riv_gdf = lakes_res_riv_gdf.to_crs(co_gdf.crs)

        # Clip lakes/reservoirs to Colorado boundary
        lakes_res_riv_co_gdf = gpd.clip(lakes_res_riv_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = lakes_res_riv_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            lakes_res_riv_co_gdf[c] = lakes_res_riv_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

        lakes_res_riv_co_gdf_str = lakes_res_riv_co_gdf.to_json()
        lakes_res_riv_co_src = GeoJSONDataSource(
            geojson=lakes_res_riv_co_gdf_str
        )

        elapsed_time_lk = time.time() - start_time_lk
        min = int(elapsed_time_lk // 60)
        sec = np.round(elapsed_time_lk % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado state public access properties (PAP) shape file
        # ---------------------------------------------------------------------
        # "https://geodata-cpw.hub.arcgis.com/datasets/" +
        # "f227d7a73ecd4a3cae5e61a83ddd76a9/about"
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
        # "https://geodata-cpw.hub.arcgis.com/datasets/" +
        # "f227d7a73ecd4a3cae5e61a83ddd76a9/about"
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

        # ---------------------------------------------------------------------
        # Create Colorado national parks and monuments shape file
        # ---------------------------------------------------------------------
        # Colorado has one national park (Rocky Mountain National Park) and
        # five National Monuments (Colorado National Monument, Dinosaur
        # National Monument, Hovenweep National Monument, Canyons of the
        # Ancients National Monument, and Yucca House National Monument).
        #
        # "https://public-nps.opendata.arcgis.com/datasets/" +
        # "nps::nps-land-resources-division-boundary-and-tract-data-service/" +
        # "explore?layer=2&location=23.210911%2C-95.642431%2C3"
        # ---------------------------------------------------------------------
        print("")
        print("Creating national parks and monuments shapefile")
        start_time_np = time.time()
        nat_parks_shapefile_path = os.path.join(
            data_dir, "shp", "nps_boundary", "nps_boundary.shp"
        )
        nat_parks_gdf = gpd.GeoDataFrame.from_file(nat_parks_shapefile_path)
        # Make sure parks are in the same CRS as Colorado (and therefore the
        # figure)
        nat_parks_gdf = nat_parks_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        nat_parks_co_gdf = gpd.clip(nat_parks_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = nat_parks_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            nat_parks_co_gdf[c] = nat_parks_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        nat_parks_co_gdf_str = nat_parks_co_gdf.to_json()
        nat_parks_co_src = GeoJSONDataSource(geojson=nat_parks_co_gdf_str)

        elapsed_time_np = time.time() - start_time_np
        min = int(elapsed_time_np // 60)
        sec = np.round(elapsed_time_np % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado ambulance districts shape file
        # ---------------------------------------------------------------------
        # Colorado ambulance districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating ambulance districts shapefile")
        start_time_amb = time.time()
        amb_geojson_path = os.path.join(
            data_dir, "geojson", "layer_0.geojson"
        )
        amb_gdf = gpd.GeoDataFrame.from_file(amb_geojson_path)
        # Make sure ambulance districts are in the same CRS as Colorado (and
        # therefore the figure)
        amb_gdf = amb_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        amb_co_gdf = gpd.clip(amb_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = amb_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            amb_co_gdf[c] = amb_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        amb_co_gdf_str = amb_co_gdf.to_json()
        amb_co_src = GeoJSONDataSource(geojson=amb_co_gdf_str)

        elapsed_time_amb = time.time() - start_time_amb
        min = int(elapsed_time_amb // 60)
        sec = np.round(elapsed_time_amb % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado business improvement districts shape file
        # ---------------------------------------------------------------------
        # Colorado business improvement districts geojson file was scraped
        # from the Colorado Property Tax Map interactive web map on March 29,
        # 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating business improvement districts shapefile")
        start_time_bimp = time.time()
        bimp_geojson_path = os.path.join(
            data_dir, "geojson", "layer_1.geojson"
        )
        bimp_gdf = gpd.GeoDataFrame.from_file(bimp_geojson_path)
        # Make sure business improvement districts are in the same CRS as
        # Colorado (and therefore the figure)
        bimp_gdf = bimp_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        bimp_co_gdf = gpd.clip(bimp_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = bimp_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            bimp_co_gdf[c] = bimp_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        bimp_co_gdf_str = bimp_co_gdf.to_json()
        bimp_co_src = GeoJSONDataSource(geojson=bimp_co_gdf_str)

        elapsed_time_bimp = time.time() - start_time_bimp
        min = int(elapsed_time_bimp // 60)
        sec = np.round(elapsed_time_bimp % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado cemetery districts shape file
        # ---------------------------------------------------------------------
        # Colorado cemetery districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating cemetery districts shapefile")
        start_time_cem = time.time()
        cem_geojson_path = os.path.join(
            data_dir, "geojson", "layer_2.geojson"
        )
        cem_gdf = gpd.GeoDataFrame.from_file(cem_geojson_path)
        # Make sure cemetery districts are in the same CRS as Colorado (and
        # therefore the figure)
        cem_gdf = cem_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        cem_co_gdf = gpd.clip(cem_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = cem_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            cem_co_gdf[c] = cem_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        cem_co_gdf_str = cem_co_gdf.to_json()
        cem_co_src = GeoJSONDataSource(geojson=cem_co_gdf_str)

        elapsed_time_cem = time.time() - start_time_cem
        min = int(elapsed_time_cem // 60)
        sec = np.round(elapsed_time_cem % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado conservation districts shape file
        # [TODO: Why does this display as two tones? Maybe overlapping layers?]
        # ---------------------------------------------------------------------
        # Colorado conservation districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating conservation districts shapefile")
        start_time_con = time.time()
        con_geojson_path = os.path.join(
            data_dir, "geojson", "layer_3.geojson"
        )
        con_gdf = gpd.GeoDataFrame.from_file(con_geojson_path)
        # Make sure conservation districts are in the same CRS as Colorado (and
        # therefore the figure)
        con_gdf = con_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # con_co_gdf = gpd.clip(con_gdf, co_gdf)
        con_co_gdf = con_gdf.copy()

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = con_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            con_co_gdf[c] = con_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        con_co_gdf_str = con_co_gdf.to_json()
        con_co_src = GeoJSONDataSource(geojson=con_co_gdf_str)

        elapsed_time_con = time.time() - start_time_con
        min = int(elapsed_time_con // 60)
        sec = np.round(elapsed_time_con % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado county disposal district shape file
        # ---------------------------------------------------------------------
        # Colorado county disposal district geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating county disposal district shapefile")
        start_time_cdsp = time.time()
        cdsp_geojson_path = os.path.join(
            data_dir, "geojson", "layer_5.geojson"
        )
        cdsp_gdf = gpd.GeoDataFrame.from_file(cdsp_geojson_path)
        # Make sure county disposal district is in the same CRS as Colorado
        # (and therefore the figure)
        cdsp_gdf = cdsp_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        cdsp_co_gdf = gpd.clip(cdsp_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = cdsp_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            cdsp_co_gdf[c] = cdsp_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        cdsp_co_gdf_str = cdsp_co_gdf.to_json()
        cdsp_co_src = GeoJSONDataSource(geojson=cdsp_co_gdf_str)

        elapsed_time_cdsp = time.time() - start_time_cdsp
        min = int(elapsed_time_cdsp // 60)
        sec = np.round(elapsed_time_cdsp % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado county pest control districts shape file
        # ---------------------------------------------------------------------
        # Colorado county pest control districts geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating county pest control districts shapefile")
        start_time_cpst = time.time()
        cpst_geojson_path = os.path.join(
            data_dir, "geojson", "layer_6.geojson"
        )
        cpst_gdf = gpd.GeoDataFrame.from_file(cpst_geojson_path)
        # Make sure county pest control districts are in the same CRS as
        # Colorado (and therefore the figure)
        cpst_gdf = cpst_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        cpst_co_gdf = gpd.clip(cpst_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = cpst_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            cpst_co_gdf[c] = cpst_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        cpst_co_gdf_str = cpst_co_gdf.to_json()
        cpst_co_src = GeoJSONDataSource(geojson=cpst_co_gdf_str)

        elapsed_time_cpst = time.time() - start_time_cpst
        min = int(elapsed_time_cpst // 60)
        sec = np.round(elapsed_time_cpst % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado downtown development districts shape file
        # ---------------------------------------------------------------------
        # Colorado downtown development districts geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating downtown development districts shapefile")
        start_time_ddev = time.time()
        ddev_geojson_path = os.path.join(
            data_dir, "geojson", "layer_7.geojson"
        )
        ddev_gdf = gpd.GeoDataFrame.from_file(ddev_geojson_path)
        # Make sure downtown development districts are in the same CRS as
        # Colorado (and therefore the figure)
        ddev_gdf = ddev_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        ddev_co_gdf = gpd.clip(ddev_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = ddev_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            ddev_co_gdf[c] = ddev_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        ddev_co_gdf_str = ddev_co_gdf.to_json()
        ddev_co_src = GeoJSONDataSource(geojson=ddev_co_gdf_str)

        elapsed_time_ddev = time.time() - start_time_ddev
        min = int(elapsed_time_ddev // 60)
        sec = np.round(elapsed_time_ddev % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado education districts shape file
        # ---------------------------------------------------------------------
        # Colorado education districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating education districts shapefile")
        start_time_educ = time.time()
        educ_geojson_path = os.path.join(
            data_dir, "geojson", "layer_4.geojson"
        )
        educ_gdf = gpd.GeoDataFrame.from_file(educ_geojson_path)
        # Make sure education districts are in the same CRS as Colorado (and
        # therefore the figure)
        educ_gdf = educ_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        educ_co_gdf = gpd.clip(educ_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = educ_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            educ_co_gdf[c] = educ_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        educ_co_gdf_str = educ_co_gdf.to_json()
        educ_co_src = GeoJSONDataSource(geojson=educ_co_gdf_str)

        elapsed_time_educ = time.time() - start_time_educ
        min = int(elapsed_time_educ // 60)
        sec = np.round(elapsed_time_educ % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado fire protection districts shape file
        # ---------------------------------------------------------------------
        # Colorado fire protection districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating fire protection districts shapefile")
        start_time_fprt = time.time()
        fprt_geojson_path = os.path.join(
            data_dir, "geojson", "layer_9.geojson"
        )
        fprt_gdf = gpd.GeoDataFrame.from_file(fprt_geojson_path)
        # Make sure fire protection districts are in the same CRS as Colorado
        # (and therefore the figure)
        fprt_gdf = fprt_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # fprt_co_gdf = gpd.clip(fprt_gdf, co_gdf)
        fprt_co_gdf = fprt_gdf.copy()

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = fprt_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            fprt_co_gdf[c] = fprt_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        fprt_co_gdf_str = fprt_co_gdf.to_json()
        fprt_co_src = GeoJSONDataSource(geojson=fprt_co_gdf_str)

        elapsed_time_fprt = time.time() - start_time_fprt
        min = int(elapsed_time_fprt // 60)
        sec = np.round(elapsed_time_fprt % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado general improvement districts shape file
        # ---------------------------------------------------------------------
        # Colorado general improvement districts geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating general improvement districts shapefile")
        start_time_gimp = time.time()
        gimp_geojson_path = os.path.join(
            data_dir, "geojson", "layer_10.geojson"
        )
        gimp_gdf = gpd.GeoDataFrame.from_file(gimp_geojson_path)
        # Make sure general improvement districts are in the same CRS as Colorado
        # (and therefore the figure)
        gimp_gdf = gimp_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # gimp_co_gdf = gpd.clip(gimp_gdf, co_gdf)
        gimp_co_gdf = gimp_gdf.copy()

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = gimp_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            gimp_co_gdf[c] = gimp_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        gimp_co_gdf_str = gimp_co_gdf.to_json()
        gimp_co_src = GeoJSONDataSource(geojson=gimp_co_gdf_str)

        elapsed_time_gimp = time.time() - start_time_gimp
        min = int(elapsed_time_gimp // 60)
        sec = np.round(elapsed_time_gimp % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado health service districts shape file
        # ---------------------------------------------------------------------
        # Colorado health service districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating health service districts shapefile")
        start_time_hsd = time.time()
        hsd_geojson_path = os.path.join(
            data_dir, "geojson", "layer_11.geojson"
        )
        hsd_gdf = gpd.GeoDataFrame.from_file(hsd_geojson_path)
        # Make sure health service districts are in the same CRS as Colorado
        # (and therefore the figure)
        hsd_gdf = hsd_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        hsd_co_gdf = gpd.clip(hsd_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = hsd_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            hsd_co_gdf[c] = hsd_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        hsd_co_gdf_str = hsd_co_gdf.to_json()
        hsd_co_src = GeoJSONDataSource(geojson=hsd_co_gdf_str)

        elapsed_time_hsd = time.time() - start_time_hsd
        min = int(elapsed_time_hsd // 60)
        sec = np.round(elapsed_time_hsd % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado housing authority shape file
        # ---------------------------------------------------------------------
        # Colorado housing authority geojson file was scraped from the Colorado
        # Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating housing authority shapefile")
        start_time_haut = time.time()
        haut_geojson_path = os.path.join(
            data_dir, "geojson", "layer_12.geojson"
        )
        haut_gdf = gpd.GeoDataFrame.from_file(haut_geojson_path)
        # Make sure housing authority authority is in the same CRS as Colorado
        # (and therefore the figure)
        haut_gdf = haut_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        haut_co_gdf = gpd.clip(haut_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = haut_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            haut_co_gdf[c] = haut_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        haut_co_gdf_str = haut_co_gdf.to_json()
        haut_co_src = GeoJSONDataSource(geojson=haut_co_gdf_str)

        elapsed_time_haut = time.time() - start_time_haut
        min = int(elapsed_time_haut // 60)
        sec = np.round(elapsed_time_haut % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado law enforcement authorities shape file
        # ---------------------------------------------------------------------
        # Colorado law enforcement authorities geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating law enforcement authorities shapefile")
        start_time_lenf = time.time()
        lenf_geojson_path = os.path.join(
            data_dir, "geojson", "layer_13.geojson"
        )
        lenf_gdf = gpd.GeoDataFrame.from_file(lenf_geojson_path)
        # Make sure law enforcement authorities are in the same CRS as Colorado
        # (and therefore the figure)
        lenf_gdf = lenf_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # lenf_co_gdf = gpd.clip(lenf_gdf, co_gdf)
        lenf_co_gdf = lenf_gdf.copy()

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = lenf_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            lenf_co_gdf[c] = lenf_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        lenf_co_gdf_str = lenf_co_gdf.to_json()
        lenf_co_src = GeoJSONDataSource(geojson=lenf_co_gdf_str)

        elapsed_time_lenf = time.time() - start_time_lenf
        min = int(elapsed_time_lenf // 60)
        sec = np.round(elapsed_time_lenf % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado library districts shape file
        # ---------------------------------------------------------------------
        # Colorado library districts geojson file was scraped from the Colorado
        # Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating library districts shapefile")
        start_time_lib = time.time()
        lib_geojson_path = os.path.join(
            data_dir, "geojson", "layer_14.geojson"
        )
        lib_gdf = gpd.GeoDataFrame.from_file(lib_geojson_path)
        # Make sure library districts are in the same CRS as Colorado (and
        # therefore the figure)
        lib_gdf = lib_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        lib_co_gdf = gpd.clip(lib_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = lib_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            lib_co_gdf[c] = lib_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        lib_co_gdf_str = lib_co_gdf.to_json()
        lib_co_src = GeoJSONDataSource(geojson=lib_co_gdf_str)

        elapsed_time_lib = time.time() - start_time_lib
        min = int(elapsed_time_lib // 60)
        sec = np.round(elapsed_time_lib % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado metropolitan districts shape file
        # ---------------------------------------------------------------------
        # Colorado metropolitan districts geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating metropolitan districts shapefile")
        start_time_met = time.time()
        met_geojson_path = os.path.join(
            data_dir, "geojson", "layer_15.geojson"
        )
        met_gdf = gpd.GeoDataFrame.from_file(met_geojson_path)
        # Make sure metropolitan districts are in the same CRS as Colorado (and
        # therefore the figure)
        met_gdf = met_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        met_co_gdf = gpd.clip(met_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = met_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            met_co_gdf[c] = met_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        met_co_gdf_str = met_co_gdf.to_json()
        met_co_src = GeoJSONDataSource(geojson=met_co_gdf_str)

        elapsed_time_met = time.time() - start_time_met
        min = int(elapsed_time_met // 60)
        sec = np.round(elapsed_time_met % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado municipal boundaries shape file
        # ---------------------------------------------------------------------
        # Colorado municipal boundaries geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating municipal boundaries shapefile")
        start_time_muni = time.time()
        muni_geojson_path = os.path.join(
            data_dir, "geojson", "layer_16.geojson"
        )
        muni_gdf = gpd.GeoDataFrame.from_file(muni_geojson_path)
        # Make sure municipal boundaries are in the same CRS as Colorado
        # (and therefore the figure)
        muni_gdf = muni_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        muni_co_gdf = gpd.clip(muni_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = muni_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            muni_co_gdf[c] = muni_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        muni_co_gdf_str = muni_co_gdf.to_json()
        muni_co_src = GeoJSONDataSource(geojson=muni_co_gdf_str)

        elapsed_time_muni = time.time() - start_time_muni
        min = int(elapsed_time_muni // 60)
        sec = np.round(elapsed_time_muni % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado park and recreation districts shape file
        # ---------------------------------------------------------------------
        # Colorado park and recreation districts geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating park and recreation districts shapefile")
        start_time_prec = time.time()
        prec_geojson_path = os.path.join(
            data_dir, "geojson", "layer_17.geojson"
        )
        prec_gdf = gpd.GeoDataFrame.from_file(prec_geojson_path)
        # Make sure park and recreation districts are in the same CRS as
        # Colorado (and therefore the figure)
        prec_gdf = prec_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # prec_co_gdf = gpd.clip(prec_gdf, co_gdf)
        prec_co_gdf = prec_gdf.copy()

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = prec_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            prec_co_gdf[c] = prec_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        prec_co_gdf_str = prec_co_gdf.to_json()
        prec_co_src = GeoJSONDataSource(geojson=prec_co_gdf_str)

        elapsed_time_prec = time.time() - start_time_prec
        min = int(elapsed_time_prec // 60)
        sec = np.round(elapsed_time_prec % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado public improvement districts shape file
        # ---------------------------------------------------------------------
        # Colorado public improvement districts geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating public improvement districts shapefile")
        start_time_pimp = time.time()
        pimp_geojson_path = os.path.join(
            data_dir, "geojson", "layer_18.geojson"
        )
        pimp_gdf = gpd.GeoDataFrame.from_file(pimp_geojson_path)
        # Make sure public improvement districts are in the same CRS as
        # Colorado (and therefore the figure)
        pimp_gdf = pimp_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        pimp_co_gdf = gpd.clip(pimp_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = pimp_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            pimp_co_gdf[c] = pimp_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        pimp_co_gdf_str = pimp_co_gdf.to_json()
        pimp_co_src = GeoJSONDataSource(geojson=pimp_co_gdf_str)

        elapsed_time_pimp = time.time() - start_time_pimp
        min = int(elapsed_time_pimp // 60)
        sec = np.round(elapsed_time_pimp % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado special districts shape file
        # [TODO: Why does this display as two tones? Maybe overlapping layers?]
        # ---------------------------------------------------------------------
        # Colorado special districts geojson file was scraped from the Colorado
        # Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating special districts shapefile")
        start_time_spec = time.time()
        spec_geojson_path = os.path.join(
            data_dir, "geojson", "layer_19.geojson"
        )
        spec_gdf = gpd.GeoDataFrame.from_file(spec_geojson_path)
        # Make sure special districts are in the same CRS as
        # Colorado (and therefore the figure)
        spec_gdf = spec_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        spec_co_gdf = gpd.clip(spec_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = spec_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            spec_co_gdf[c] = spec_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        spec_co_gdf_str = spec_co_gdf.to_json()
        spec_co_src = GeoJSONDataSource(geojson=spec_co_gdf_str)

        elapsed_time_spec = time.time() - start_time_spec
        min = int(elapsed_time_spec // 60)
        sec = np.round(elapsed_time_spec % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado transportation authorities shape file
        # ---------------------------------------------------------------------
        # Colorado transportation authorities geojson file was scraped from the
        # Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating transportation authorities shapefile")
        start_time_tran = time.time()
        tran_geojson_path = os.path.join(
            data_dir, "geojson", "layer_27.geojson"
        )
        tran_gdf = gpd.GeoDataFrame.from_file(tran_geojson_path)
        # Make sure transportation authorities are in the same CRS as
        # Colorado (and therefore the figure)
        tran_gdf = tran_gdf.to_crs(co_gdf.crs)
        # Clip to Colorado so stray polygons don't expand bounds
        tran_co_gdf = gpd.clip(tran_gdf, co_gdf)

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = tran_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            tran_co_gdf[c] = tran_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        tran_co_gdf_str = tran_co_gdf.to_json()
        tran_co_src = GeoJSONDataSource(geojson=tran_co_gdf_str)

        elapsed_time_tran = time.time() - start_time_tran
        min = int(elapsed_time_tran // 60)
        sec = np.round(elapsed_time_tran % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Create Colorado water and sanitation districts shape file
        # [TODO: Why does this display as two tones? Maybe overlapping layers?]
        # ---------------------------------------------------------------------
        # Colorado water and sanitation districts geojson file was scraped from
        # the Colorado Property Tax Map interactive web map on March 29, 2026.
        # https://gis.colorado.gov/proptaxmap/?page=MapView
        # ---------------------------------------------------------------------
        print("")
        print("Creating water and sanitation districts shapefile")
        start_time_wsan = time.time()
        wsan_geojson_path = os.path.join(
            data_dir, "geojson", "layer_20.geojson"
        )
        wsan_gdf = gpd.GeoDataFrame.from_file(wsan_geojson_path)
        # Make sure water and sanitation districts are in the same CRS as
        # Colorado (and therefore the figure)
        wsan_gdf = wsan_gdf.to_crs(co_gdf.crs)
        # # Clip to Colorado so stray polygons don't expand bounds
        # wsan_co_gdf = gpd.clip(wsan_gdf, co_gdf)
        wsan_co_gdf = wsan_gdf.copy()

        # Find datetime-ish columns and convert to ISO strings
        dt_cols = wsan_co_gdf.select_dtypes(
            include=["datetime64[ns]", "datetime64[ns, UTC]"]
        ).columns
        for c in dt_cols:
            wsan_co_gdf[c] = wsan_co_gdf[c].dt.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        wsan_co_gdf_str = wsan_co_gdf.to_json()
        wsan_co_src = GeoJSONDataSource(geojson=wsan_co_gdf_str)

        elapsed_time_wsan = time.time() - start_time_wsan
        min = int(elapsed_time_wsan // 60)
        sec = np.round(elapsed_time_wsan % 60, 1)
        print(f"took {min} minutes and {sec} seconds.")

        # ---------------------------------------------------------------------
        # Save gdf and geojson data files
        # ---------------------------------------------------------------------
        # Create dictionaries of GeoDataFrames and GeoJSONDataSources for all
        # layers
        gdf_dict = {
            "co_gdf": co_gdf,
            "co_counties_gdf": co_counties_gdf,
            "lakes_res_riv_co_gdf": lakes_res_riv_co_gdf,
            "state_pap_co_gdf": state_pap_co_gdf,
            "state_ftp_co_gdf": state_ftp_co_gdf,
            "nat_parks_co_gdf": nat_parks_co_gdf,
            "amb_co_gdf": amb_co_gdf,
            "bimp_co_gdf": bimp_co_gdf,
            "con_co_gdf": con_co_gdf,
            "cdsp_co_gdf": cdsp_co_gdf,
            "cpst_co_gdf": cpst_co_gdf,
            "ddev_co_gdf": ddev_co_gdf,
            "educ_co_gdf": educ_co_gdf,
            "fprt_co_gdf": fprt_co_gdf,
            "gimp_co_gdf": gimp_co_gdf,
            "hsd_co_gdf": hsd_co_gdf,
            "haut_co_gdf": haut_co_gdf,
            "lenf_co_gdf": lenf_co_gdf,
            "lib_co_gdf": lib_co_gdf,
            "met_co_gdf": met_co_gdf,
            "muni_co_gdf": muni_co_gdf,
            "prec_co_gdf": prec_co_gdf,
            "pimp_co_gdf": pimp_co_gdf,
            "spec_co_gdf": spec_co_gdf,
            "tran_co_gdf": tran_co_gdf,
            "wsan_co_gdf": wsan_co_gdf
        }
        geojson_dict = {
            "co_gdf_str": co_gdf_str,
            "co_counties_gdf_str": co_counties_gdf_str,
            "lakes_res_riv_co_gdf_str": lakes_res_riv_co_gdf_str,
            "state_pap_co_gdf_str": state_pap_co_gdf_str,
            "state_ftp_co_gdf_str": state_ftp_co_gdf_str,
            "nat_parks_co_gdf_str": nat_parks_co_gdf_str,
            "amb_co_gdf_str": amb_co_gdf_str,
            "bimp_co_gdf_str": bimp_co_gdf_str,
            "cem_co_gdf_str": cem_co_gdf_str,
            "con_co_gdf_str": con_co_gdf_str,
            "cdsp_co_gdf_str": cdsp_co_gdf_str,
            "cpst_co_gdf_str": cpst_co_gdf_str,
            "ddev_co_gdf_str": ddev_co_gdf_str,
            "educ_co_gdf_str": educ_co_gdf_str,
            "fprt_co_gdf_str": fprt_co_gdf_str,
            "gimp_co_gdf_str": gimp_co_gdf_str,
            "hsd_co_gdf_str": hsd_co_gdf_str,
            "haut_co_gdf_str": haut_co_gdf_str,
            "lenf_co_gdf_str": lenf_co_gdf_str,
            "lib_co_gdf_str": lib_co_gdf_str,
            "met_co_gdf_str": met_co_gdf_str,
            "muni_co_gdf_str": muni_co_gdf_str,
            "prec_co_gdf_str": prec_co_gdf_str,
            "pimp_co_gdf_str": pimp_co_gdf_str,
            "spec_co_gdf_str": spec_co_gdf_str,
            "tran_co_gdf_str": tran_co_gdf_str,
            "wsan_co_gdf_str": wsan_co_gdf_str
        }
        src_dict = {
            "co_src": co_src,
            "co_counties_src": co_counties_src,
            "lakes_res_riv_co_src": lakes_res_riv_co_src,
            "state_pap_co_src": state_pap_co_src,
            "state_ftp_co_src": state_ftp_co_src,
            "nat_parks_co_src": nat_parks_co_src,
            "amb_co_src": amb_co_src,
            "bimp_co_src": bimp_co_src,
            "cem_co_src": cem_co_src,
            "con_co_src": con_co_src,
            "cdsp_co_src": cdsp_co_src,
            "cpst_co_src": cpst_co_src,
            "ddev_co_src": ddev_co_src,
            "educ_co_src": educ_co_src,
            "fprt_co_src": fprt_co_src,
            "gimp_co_src": gimp_co_src,
            "hsd_co_src": hsd_co_src,
            "haut_co_src": haut_co_src,
            "lenf_co_src": lenf_co_src,
            "lib_co_src": lib_co_src,
            "met_co_src": met_co_src,
            "muni_co_src": muni_co_src,
            "prec_co_src": prec_co_src,
            "pimp_co_src": pimp_co_src,
            "spec_co_src": spec_co_src,
            "tran_co_src": tran_co_src,
            "wsan_co_src": wsan_co_src
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
            "lakes_res_riv_co_gdf",
            "state_pap_co_gdf",
            "state_ftp_co_gdf",
            "nat_parks_co_gdf",
            "amb_co_gdf",
            "bimp_co_gdf",
            "cem_co_gdf",
            "con_co_gdf",
            "cdsp_co_gdf",
            "cpst_co_gdf",
            "ddev_co_gdf",
            "educ_co_gdf",
            "fprt_co_gdf",
            "gimp_co_gdf",
            "hsd_co_gdf",
            "haut_co_gdf",
            "lenf_co_gdf",
            "lib_co_gdf",
            "met_co_gdf",
            "muni_co_gdf",
            "prec_co_gdf",
            "pimp_co_gdf",
            "spec_co_gdf",
            "tran_co_gdf",
            "wsan_co_gdf"
        ]
        gdf_dict = {
            os.name: pickle.load(
                open(os.path.join(data_dir, "gdf", f"{name}.pkl"), "rb")
            ) for name in gdf_name_list
        }

        geojson_name_list = [
            "co_gdf_str",
            "co_counties_gdf_str",
            "lakes_res_riv_co_gdf_str",
            "state_pap_co_gdf_str",
            "state_ftp_co_gdf_str",
            "nat_parks_co_gdf_str",
            "amb_co_gdf_str",
            "bimp_co_gdf_str",
            "cem_co_gdf_str",
            "con_co_gdf_str",
            "cdsp_co_gdf_str",
            "cpst_co_gdf_str",
            "ddev_co_gdf_str",
            "educ_co_gdf_str",
            "fprt_co_gdf_str",
            "gimp_co_gdf_str",
            "hsd_co_gdf_str",
            "haut_co_gdf_str",
            "lenf_co_gdf_str",
            "lib_co_gdf_str",
            "met_co_gdf_str",
            "muni_co_gdf_str",
            "prec_co_gdf_str",
            "pimp_co_gdf_str",
            "spec_co_gdf_str",
            "tran_co_gdf_str",
            "wsan_co_gdf_str"
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
    fig1.patches(
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
    print("Fig 1 Status: Plotting lakes_res_riv_co_src")
    r_lakes = fig1.patches(
        "xs", "ys",
        source=src_dict["lakes_res_riv_co_src"],
        fill_color="blue",
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.5,
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

    # Colorado national park boundaries
    print("Fig 1 Status: Plotting nat_parks_co_src")
    r_nat_parks = fig1.patches(
        "xs", "ys",
        source=src_dict["nat_parks_co_src"],
        fill_color="saddlebrown",
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado ambulance districts boundaries
    print("Fig 1 Status: Plotting amb_co_src")
    r_amb = fig1.patches(
        "xs", "ys",
        source=src_dict["amb_co_src"],
        fill_color=Viridis256[12 * 1 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado business improvement districts boundaries
    print("Fig 1 Status: Plotting bimp_co_src")
    r_bimp = fig1.patches(
        "xs", "ys",
        source=src_dict["bimp_co_src"],
        fill_color=Viridis256[12*2 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado cemetery districts boundaries
    print("Fig 1 Status: Plotting cem_co_src")
    r_cem = fig1.patches(
        "xs", "ys",
        source=src_dict["cem_co_src"],
        fill_color=Viridis256[12*3 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado conservation districts boundaries
    print("Fig 1 Status: Plotting con_co_src")
    r_con = fig1.patches(
        "xs", "ys",
        source=src_dict["con_co_src"],
        fill_color=Viridis256[12*4 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado county disposal district boundaries
    print("Fig 1 Status: Plotting cdsp_co_src")
    r_cdsp = fig1.patches(
        "xs", "ys",
        source=src_dict["cdsp_co_src"],
        fill_color=Viridis256[12*5 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado county pest control districts boundaries
    print("Fig 1 Status: Plotting cpst_co_src")
    r_cpst = fig1.patches(
        "xs", "ys",
        source=src_dict["cpst_co_src"],
        fill_color=Viridis256[12*6 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado downtown development districts boundaries
    print("Fig 1 Status: Plotting ddev_co_src")
    r_ddev = fig1.patches(
        "xs", "ys",
        source=src_dict["ddev_co_src"],
        fill_color=Viridis256[12*7 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado education districts boundaries
    print("Fig 1 Status: Plotting educ_co_src")
    r_educ = fig1.patches(
        "xs", "ys",
        source=src_dict["educ_co_src"],
        fill_color=Viridis256[12*8 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado fire protection districts boundaries
    print("Fig 1 Status: Plotting fprt_co_src")
    r_fprt = fig1.patches(
        "xs", "ys",
        source=src_dict["fprt_co_src"],
        fill_color=Viridis256[12*9 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado general improvement districts boundaries
    print("Fig 1 Status: Plotting gimp_co_src")
    r_gimp = fig1.patches(
        "xs", "ys",
        source=src_dict["gimp_co_src"],
        fill_color=Viridis256[12*10 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado health service districts boundaries
    print("Fig 1 Status: Plotting hsd_co_src")
    r_hsd = fig1.patches(
        "xs", "ys",
        source=src_dict["hsd_co_src"],
        fill_color=Viridis256[12*11 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado housing authority boundaries
    print("Fig 1 Status: Plotting haut_co_src")
    r_haut = fig1.patches(
        "xs", "ys",
        source=src_dict["haut_co_src"],
        fill_color=Viridis256[12*12 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado law enforcement authorities boundaries
    print("Fig 1 Status: Plotting lenf_co_src")
    r_lenf = fig1.patches(
        "xs", "ys",
        source=src_dict["lenf_co_src"],
        fill_color=Viridis256[12*13 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado library districts boundaries
    print("Fig 1 Status: Plotting lib_co_src")
    r_lib = fig1.patches(
        "xs", "ys",
        source=src_dict["lib_co_src"],
        fill_color=Viridis256[12*14 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado metropolitan districts boundaries
    print("Fig 1 Status: Plotting met_co_src")
    r_met = fig1.patches(
        "xs", "ys",
        source=src_dict["met_co_src"],
        fill_color=Viridis256[12*15 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado municipal boundaries
    print("Fig 1 Status: Plotting muni_co_src")
    r_muni = fig1.patches(
        "xs", "ys",
        source=src_dict["muni_co_src"],
        fill_color=Viridis256[12*16 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado park and recreation districts boundaries
    print("Fig 1 Status: Plotting prec_co_src")
    r_prec = fig1.patches(
        "xs", "ys",
        source=src_dict["prec_co_src"],
        fill_color=Viridis256[12*17 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado public improvement districts boundaries
    print("Fig 1 Status: Plotting pimp_co_src")
    r_pimp = fig1.patches(
        "xs", "ys",
        source=src_dict["pimp_co_src"],
        fill_color=Viridis256[12*18 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado special districts boundaries
    print("Fig 1 Status: Plotting spec_co_src")
    r_spec = fig1.patches(
        "xs", "ys",
        source=src_dict["spec_co_src"],
        fill_color=Viridis256[12*19 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado transportation authorities boundaries
    print("Fig 1 Status: Plotting tran_co_src")
    r_tran = fig1.patches(
        "xs", "ys",
        source=src_dict["tran_co_src"],
        fill_color=Viridis256[12*20 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    # Colorado water and sanitation districts boundaries
    print("Fig 1 Status: Plotting wsan_co_src")
    r_wsan = fig1.patches(
        "xs", "ys",
        source=src_dict["wsan_co_src"],
        fill_color=Viridis256[12*21 - 5],
        fill_alpha=0.4,
        line_alpha=0.8,
        line_width=0.2,
        muted_alpha=0.0
    )

    fig1_legend = Legend(items=[
        ("Colorado counties", [r_counties]),
        ("Lakes, reservoirs, rivers", [r_lakes]),
        ("State Public Access Properties", [r_state_pap]),
        ("State Fee Title Parcels", [r_state_ftp]),
        ("National Parks and Monuments", [r_nat_parks]),
        ("Ambulance Districts", [r_amb]),
        ("Business Improvement Districts", [r_bimp]),
        ("Cemetery Districts", [r_cem]),
        ("Conservation Districts", [r_con]),
        ("County Disposal District", [r_cdsp]),
        ("County Pest Control Districts", [r_cpst]),
        ("Downtown Development Districts", [r_ddev]),
        ("Education Districts", [r_educ]),
        ("Fire Protection Districts", [r_fprt]),
        ("General Improvement Districts", [r_gimp]),
        ("Health Service Districts", [r_hsd]),
        ("Housing Authority", [r_haut]),
        ("Law Enforcement Authorities", [r_lenf]),
        ("Library Districts", [r_lib]),
        ("Metropolitan Districts", [r_met]),
        ("Municipal Boundaries", [r_muni]),
        ("Park and Recreation Districts", [r_prec]),
        ("Public Improvement Districts", [r_pimp]),
        ("Special Districts", [r_spec]),
        ("Transportation Authorities", [r_tran]),
        ("Water and Sanitation Districts", [r_wsan])
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

    show(fig1)
