import os
from datetime import date

import pandas as pd

RUTA_SII = 'crudos/base_de_datos_facturas/SII'
RUTA_ACEPTA = 'crudos/base_de_datos_facturas/ACEPTA'
RUTA_SIGFE_REPORTS = 'crudos/base_de_datos_facturas/SIGFE'
RUTA_TURBO = 'crudos/base_de_datos_facturas/TURBO'
RUTA_SCI = 'crudos/base_de_datos_facturas/SCI'
RUTA_OBSERVACIONES = 'crudos/base_de_datos_facturas/OBSERVACIONES'


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

    def convertir_montos_nc_y_rechazadas_a_negativos(self, df):
        tmp = df.copy()

        columnas_a_cambiar = ['Monto Exento', 'Monto Neto', 'Monto IVA Recuperable', 'Monto Total']

        nc_y_rechazadas = df.query('`Tipo Doc` == 61 or `Tipo Doc` == 56').copy()
        nc_y_rechazadas[columnas_a_cambiar] = nc_y_rechazadas[columnas_a_cambiar] * -1

        tmp.loc[nc_y_rechazadas.index, columnas_a_cambiar] = nc_y_rechazadas[columnas_a_cambiar]

        return tmp

    def quitar_columnas_tabacos(self, df):
        tmp = df.copy()

        try:
            tmp = tmp.drop(columns=['Tabacos Puros', 'Tabacos Cigarrillos', 'Tabacos Elaborados'])

        except KeyError:
            pass

        return tmp

    def leer_un_archivo_sii(self, ruta_archivo):
        df = pd.read_csv(ruta_archivo, delimiter=';', index_col=False)
        df = df.rename(columns={'RUT Proveedor': 'RUT Emisor'})
        df = self.convertir_montos_nc_y_rechazadas_a_negativos(df)
        df = self.quitar_columnas_tabacos(df)

        return df

    def leer_todos_los_archivos_sii(self, leer_anio_actual):
        archivos = self.obtener_archivos_contenidos_en_carpeta(RUTA_SII, leer_anio_actual)
        dfs = pd.concat(map(lambda x: self.leer_un_archivo_sii(x), archivos))
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
    print(objeto.leer_todos_los_archivos_sii(False))
