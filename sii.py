import pandas as pd


class SII:
    def __init__(self):
        pass

    def leer_un_archivo_sii(self, ruta_archivo):
        df = pd.read_csv(ruta_archivo, delimiter=';', index_col=False)
        df = df.rename(columns={'RUT Proveedor': 'RUT Emisor'})
        df = self.convertir_montos_nc_y_rechazadas_a_negativos(df)
        df = self.quitar_columnas_tabacos(df)

        return df

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


class ACEPTA:
    def __init__(self):
        pass

    def leer_un_archivo_acepta(self, ruta_archivo):
        df = pd.read_excel(ruta_archivo)
        df = df.rename(columns={'emisor': 'RUT Emisor', 'folio': 'Folio'})

        return df


class SIGFEREPORTS:
    def __init__(self):
        pass

    def leer_un_archivo_sigfe_reports(self, ruta_archivo):
        df = pd.read_csv(ruta_archivo, delimiter=',', header=10)
        df = df.dropna(subset=['Folio'])
        df = df.query('`Cuenta Contable` != "Cuenta Contable"')
        df['RUT Emisor'] = self.obtener_y_limpiar_rut(df['Principal'])

        df = df.rename(columns={'Folio': 'Folio_interno', 'Número ': 'Folio'})
        df = df.reset_index()
        df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True)
        df['Folio_interno'] = df['Folio_interno'].astype('Int32')

        devengo_pago_misma_fila = self.obtener_devengo_pago_en_la_misma_fila(df)

        return devengo_pago_misma_fila

    def obtener_y_limpiar_rut(self, serie_rut):
        rut = serie_rut.str.split(' ').str[0]
        rut = rut.str.replace('.', '', regex=False)
        rut = rut.str.upper()
        rut = rut.str.strip()

        return rut

    def obtener_devengo_pago_en_la_misma_fila(self, df_sigfe_reports):
        tmp = df_sigfe_reports.copy()

        haber = (tmp['Haber'] != "0")

        tmp['Folio_interno DEVENGO'] = tmp[haber]['Folio_interno']
        tmp['Fecha DEVENGO'] = tmp[haber]['Fecha']

        debe = (tmp['Debe'] != "0")

        tmp['Folio_interno PAGO'] = tmp[debe]['Folio_interno']
        tmp['Fecha PAGO'] = tmp[debe]['Fecha']

        fecha_devengo = tmp.groupby(by=['RUT Emisor', 'Folio'])['Fecha DEVENGO'].min()
        folio_devengo = tmp.groupby(by=['RUT Emisor', 'Folio'])['Folio_interno DEVENGO'].min()
        fecha_pago = tmp.groupby(by=['RUT Emisor', 'Folio'])['Fecha PAGO'].min()
        folio_pago = tmp.groupby(by=['RUT Emisor', 'Folio'])['Folio_interno PAGO'].min()

        filas_unicas = pd.concat([fecha_devengo, folio_devengo, fecha_pago, folio_pago],
                                 axis=1).reset_index()

        return filas_unicas

    def obtener_fecha_y_folio_devengo_o_pago(self, df_sigfe_reports, a_obtener):
        tmp = df_sigfe_reports.copy()

        traductor = {'Debe': 'DEVENGO', 'Haber': 'PAGO'}
        columna_traducida = traductor[a_obtener]

        filtro_obtenido = (tmp[a_obtener] != '0')
        tmp[f'Folio_interno_{columna_traducida}'] = tmp[filtro_obtenido]['Folio_interno']
        tmp[f'Fecha_{columna_traducida}'] = tmp[filtro_obtenido]['Fecha']

        return tmp


if __name__ == '__main__':
    objeto = SIGFEREPORTS()
    print(objeto.leer_un_archivo_sigfe_reports('crudos/base_de_datos_facturas/SIGFE/SIGFE 2023.csv'))
