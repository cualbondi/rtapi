import psycopg2
import geopandas as gpd
from os import environ


database_name = environ.get('POSTGRES_DB')
database_host = environ.get('DB_HOST')
database_user = environ.get('POSTGRES_USER')
database_password = environ.get('POSTGRES_PASSWORD')

connection = 'postgres://{0}:{1}@{2}/{3}'.format(database_user,
                                                 database_password,
                                                 database_host,
                                                 database_name)


def get_recorridos():
    conn = psycopg2.connect(connection)
    query = """
        select cr.id, ruta
        from core_recorrido as cr
        join catastro_ciudad_recorridos as ccr
        on ccr.recorrido_id = cr.id
        join catastro_ciudad as cc
        on cc.id = ccr.ciudad_id
        where cc.nombre = 'Bahía Blanca'
    """

    df = gpd.read_postgis(query, conn, geom_col='ruta',
                          crs={'init': 'epsg:4326'})
    return df


def get_geom_array(geom):
    if geom.geom_type == 'LineString':
        return [geom]
    if geom.geom_type == 'Point':
        return [geom]
    return geom


def search(recorrido, A, B):
    # {"match": true, "subruta": <polyline>, "distance": 123}

    # buffsize = 0.001  # alrededor de 100mts
    buffsize = 1
    Abuff = A.buffer(buffsize)
    Bbuff = B.buffer(buffsize)
    minlength = 100000
    A_intersections = get_geom_array(recorrido.intersection(Abuff))
    B_intersections = get_geom_array(recorrido.intersection(Bbuff))
    solution = None
    for A_intersection in A_intersections:
        for B_intersection in B_intersections:
            sol = {
                "Aseg":  A_intersection,
                "Bseg":  B_intersection,
            }
            if A_intersection.geom_type == "Point":
                sol["Aproj"] = A_intersection
            else:
                sol["Aproj"] = A_intersection.interpolate(
                    A_intersection.project(A))

            if B_intersection.geom_type == "Point":
                sol["Bproj"] = B_intersection
            else:
                sol["Bproj"] = B_intersection.interpolate(
                    B_intersection.project(B))

            sol["Apos"] = recorrido.project(sol["Aproj"])
            sol["Bpos"] = recorrido.project(sol["Bproj"])

            sol["len"] = sol["Bpos"] - sol["Apos"]
            if 0 < sol["len"] < minlength:
                solution = sol
                minlength = sol["len"]

    return solution


def serialize_result(result):
    return {
        "Aseg":  result["Aseg"].to_wkt(),
        "Bseg":  result["Bseg"].to_wkt(),
        "Aproj": result["Aproj"].to_wkt(),
        "Bproj": result["Bproj"].to_wkt(),
        "len": result["len"]
    }
