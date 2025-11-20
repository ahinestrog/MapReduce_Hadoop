from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from statistics import mean, stdev

class AnalisisTemperatura(MRJob):
    
    def mapper(self, _, linea):
        try:
            datos = json.loads(linea.strip())
            zona = datos.get('climate_zone')
            temp_max = datos.get('temperature_2m_max')
            temp_min = datos.get('temperature_2m_min')
            temp_mean = datos.get('temperature_2m_mean')
            pais = datos.get('country')
            
            if not all([zona, temp_max is not None, temp_min is not None]):
                return
            
            if temp_mean is None:
                temp_mean = (float(temp_max) + float(temp_min)) / 2.0
            
            valor = {
                'temp_max': float(temp_max),
                'temp_min': float(temp_min),
                'temp_mean': float(temp_mean),
                'country': pais
            }
            
            yield (zona, json.dumps(valor))
        except:
            pass
    
    def combiner(self, zona, valores):
        temps_max = []
        temps_min = []
        temps_mean = []
        paises = set()
        
        for valor_json in valores:
            datos = json.loads(valor_json)
            temps_max.append(datos['temp_max'])
            temps_min.append(datos['temp_min'])
            temps_mean.append(datos['temp_mean'])
            paises.add(datos['country'])
        
        agregado = {
            'count': len(temps_max),
            'temps_max': temps_max,
            'temps_min': temps_min,
            'temps_mean': temps_mean,
            'countries': list(paises)
        }
        
        yield (zona, json.dumps(agregado))
    
    def reducer(self, zona, valores):
        todas_temps_max = []
        todas_temps_min = []
        todas_temps_mean = []
        todos_paises = set()
        
        for valor_json in valores:
            datos = json.loads(valor_json)
            todas_temps_max.extend(datos['temps_max'])
            todas_temps_min.extend(datos['temps_min'])
            todas_temps_mean.extend(datos['temps_mean'])
            todos_paises.update(datos['countries'])
        
        resultado = {
            'zona_climatica': zona,
            'total_registros': len(todas_temps_mean),
            'paises': sorted(list(todos_paises)),
            'temperatura_promedio': round(mean(todas_temps_mean), 2),
            'temperatura_maxima_general': round(max(todas_temps_max), 2),
            'temperatura_minima_general': round(min(todas_temps_min), 2),
            'variabilidad_temperatura': round(stdev(todas_temps_mean), 2) if len(todas_temps_mean) > 1 else 0
        }
        
        yield (zona, json.dumps(resultado))
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    AnalisisTemperatura.run()
