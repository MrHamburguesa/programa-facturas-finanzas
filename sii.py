import pandas as pd


class SII:
    def __init__(self):
        pass

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
