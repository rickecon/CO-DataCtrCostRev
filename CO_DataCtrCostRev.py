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
            "nat_parks_co_gdf": nat_parks_co_gdf
        }
        geojson_dict = {
            "co_gdf_str": co_gdf_str,
            "co_counties_gdf_str": co_counties_gdf_str,
            "lakes_res_riv_co_gdf_str": lakes_res_riv_co_gdf_str,
            "state_pap_co_gdf_str": state_pap_co_gdf_str,
            "state_ftp_co_gdf_str": state_ftp_co_gdf_str,
            "nat_parks_co_gdf_str": nat_parks_co_gdf_str
        }
        src_dict = {
            "co_src": co_src,
            "co_counties_src": co_counties_src,
            "lakes_res_riv_co_src": lakes_res_riv_co_src,
            "state_pap_co_src": state_pap_co_src,
            "state_ftp_co_src": state_ftp_co_src,
            "nat_parks_co_src": nat_parks_co_src
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
            "nat_parks_co_gdf"
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
            "nat_parks_co_gdf_str"
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

    fig1_legend = Legend(items=[
        # ("Available data center land", [r_available]),
        ("Colorado counties", [r_counties]),
        ("Lakes, reservoirs, rivers", [r_lakes]),
        ("State Public Access Properties", [r_state_pap]),
        ("State Fee Title Parcels", [r_state_ftp]),
        ("National Parks and Monuments", [r_nat_parks])
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
