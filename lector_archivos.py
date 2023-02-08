import os
from datetime import date

import pandas as pd

from sii import SII

RUTA_SII = 'crudos/base_de_datos_facturas/SII'
RUTA_ACEPTA = 'crudos/base_de_datos_facturas/ACEPTA'
RUTA_SIGFE_REPORTS = 'crudos/base_de_datos_facturas/SIGFE'
RUTA_TURBO = 'crudos/base_de_datos_facturas/TURBO'
RUTA_SCI = 'crudos/base_de_datos_facturas/SCI'
RUTA_OBSERVACIONES = 'crudos/base_de_datos_facturas/OBSERVACIONES'

BASE_FACTURAS = {'SII': SII.leer_un_archivo_sii()}


class LectorArchivos:
    def __init__(self):
        pass

    def leer_bases_de_datos_de_facturas(self, leer_anio_actual=False):
        self.leer_sii(leer_anio_actual)
        self.leer_acepta(leer_anio_actual)
        self.leer_sigfe(leer_anio_actual)
        self.leer_turbo(leer_anio_actual)
        self.leer_sci(leer_anio_actual)
        self.leer_observaciones(leer_anio_actual)

    def leer_todos_los_archivos_sii(self, leer_anio_actual):
        archivos = self.obtener_archivos_contenidos_en_carpeta(RUTA_SII, leer_anio_actual)
        dfs = pd.concat(map(lambda x: self.leer_un_archivo_sii(x), archivos))
        return dfs

    def concatenar_dfs(self, archivos_a_leer, funcion_lectura_base_de_datos, leer_anio_actual):
        archivos = self.obtener_archivos_contenidos_en_carpeta(archivos_a_leer, leer_anio_actual)
        dfs = pd.concat(map(lambda x: funcion_lectura_base_de_datos(x), archivos))
        return dfs

    def filtrar_por_anio_actual(self, lista_archivos_contenidos):
        anio_actual = str(date.today().year)
        archivos_filtrados = list(filter(lambda x: anio_actual in x,  lista_archivos_contenidos))
        return archivos_filtrados

    def obtener_archivos_contenidos_en_carpeta(self, carpeta_a_leer, filtro_anio=False):
        lista_archivos_contenidos = os.listdir(carpeta_a_leer)
        lista_archivos_contenidos = list(
            map(lambda x: f'{carpeta_a_leer}/{x}', lista_archivos_contenidos))

        if filtro_anio:
            lista_archivos_contenidos = self.filtrar_por_anio_actual(lista_archivos_contenidos)

        return lista_archivos_contenidos


if __name__ == '__main__':
    objeto = LectorArchivos()
    print(objeto.leer_todos_los_archivos_sii(False).to_csv('sii.csv', sep=';'))
