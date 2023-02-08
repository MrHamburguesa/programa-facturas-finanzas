import os
from datetime import date

class LectorArchivos:
    def __init__(self):
        pass

    def leer_bases_de_datos_de_facturas(self):
        self.leer_sii()
        self.leer_acepta()
        self.leer_sigfe()
        self.leer_turbo()
        self.leer_sci()
        self.leer_observaciones()

    def filtrar_por_anio_actual(self, lista_archivos_contenidos):
        anio_actual = str(date.today().year)
        archivos_filtrados = list(filter(lambda x: anio_actual in x,  lista_archivos_contenidos))
        return archivos_filtrados

    def obtener_archivos_contenidos_en_carpeta(self, carpeta_a_leer, filtro_anio=False):
        lista_archivos_contenidos = os.listdir(carpeta_a_leer)

        if filtro_anio:
            lista_archivos_contenidos = self.filtrar_por_anio_actual(lista_archivos_contenidos)

        return lista_archivos_contenidos

    def leer_sii(self):
        pass

if __name__ == '__main__':
    objeto = LectorArchivos()
    print(objeto.obtener_archivos_contenidos_en_carpeta('crudos/base_de_datos_facturas/SII'))