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

    def leer_un_archivo_sii(self, ruta_archivo):
        df = pd.read_csv(ruta_archivo, delimiter=';', index_col=False)

        try:
            df = df.drop(columns=['Tabacos Puros', 'Tabacos Cigarrillos', 'Tabacos Elaborados'])

        except KeyError:
            pass

        return df

    def leer_todos_los_archivos_sii(self, leer_anio_actual):
        archivos = self.obtener_archivos_contenidos_en_carpeta(RUTA_SII, leer_anio_actual)

        dfs = map(lambda x: pd.read_csv(x, delimiter=';', index_col=False), archivos)

        pass

    def filtrar_por_anio_actual(self, lista_archivos_contenidos):
        anio_actual = str(date.today().year)
        archivos_filtrados = list(filter(lambda x: anio_actual in x,  lista_archivos_contenidos))
        return archivos_filtrados

    def obtener_archivos_contenidos_en_carpeta(self, carpeta_a_leer, filtro_anio=False):
        lista_archivos_contenidos = os.listdir(carpeta_a_leer)

        if filtro_anio:
            lista_archivos_contenidos = self.filtrar_por_anio_actual(lista_archivos_contenidos)

        return lista_archivos_contenidos


if __name__ == '__main__':
    objeto = LectorArchivos()
    print(objeto.obtener_archivos_contenidos_en_carpeta('crudos/base_de_datos_facturas/SII', True))
    print(objeto.leer_un_archivo_sii(
        'crudos/base_de_datos_facturas/SII/RCV_COMPRA_NO_INCLUIR_61608402-K_201808.csv'))
