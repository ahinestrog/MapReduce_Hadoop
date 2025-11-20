from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from statistics import mean
from collections import defaultdict

class AnalisisPrecipitacion(MRJob):
    
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--min-precipitation', type=float, default=1.0)
    
    def mapper(self, _, linea):
        try:
            datos = json.loads(linea.strip())
            pais = datos.get('country')
            precipitacion = datos.get('precipitation_sum')
            zona = datos.get('climate_zone')
            
            if not all([pais, precipitacion is not None]):
                return
            
            precip_valor = float(precipitacion) if precipitacion else 0.0
            es_lluvioso = precip_valor >= self.options.min_precipitation
            
            valor = {
                'precipitation': precip_valor,
                'is_rainy': es_lluvioso,
                'climate_zone': zona
            }
            
            yield (pais, json.dumps(valor))
        except:
            pass
    
    def combiner(self, pais, valores):
        precipitaciones = []
        dias_lluviosos = 0
        total_dias = 0
        zonas = set()
        
        for valor_json in valores:
            datos = json.loads(valor_json)
            precipitaciones.append(datos['precipitation'])
            if datos['is_rainy']:
                dias_lluviosos += 1
            total_dias += 1
            zonas.add(datos['climate_zone'])
        
        agregado = {
            'precipitations': precipitaciones,
            'rainy_days': dias_lluviosos,
            'total_days': total_dias,
            'zones': list(zonas)
        }
        
        yield (pais, json.dumps(agregado))
    
    def reducer(self, pais, valores):
        todas_precipitaciones = []
        total_dias_lluviosos = 0
        total_dias = 0
        todas_zonas = set()
        
        for valor_json in valores:
            datos = json.loads(valor_json)
            todas_precipitaciones.extend(datos['precipitations'])
            total_dias_lluviosos += datos['rainy_days']
            total_dias += datos['total_days']
            todas_zonas.update(datos['zones'])
        
        resultado = {
            'pais': pais,
            'zonas_climaticas': sorted(list(todas_zonas)),
            'total_dias_analizados': total_dias,
            'precipitacion_total_mm': round(sum(todas_precipitaciones), 2),
            'precipitacion_promedio_diaria': round(mean(todas_precipitaciones), 2),
            'porcentaje_dias_lluviosos': round((total_dias_lluviosos / total_dias * 100), 2) if total_dias > 0 else 0
        }
        
        yield (pais, json.dumps(resultado))
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    AnalisisPrecipitacion.run()
