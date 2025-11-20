from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from collections import defaultdict

class AnalisisClimaExtremo(MRJob):
    
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--precip-extreme-threshold', type=float, default=50.0)
    
    def mapper(self, _, linea):
        try:
            datos = json.loads(linea.strip())
            ubicacion = datos.get('location_key')
            zona = datos.get('climate_zone')
            pais = datos.get('country')
            temp_max = datos.get('temperature_2m_max')
            temp_min = datos.get('temperature_2m_min')
            precipitacion = datos.get('precipitation_sum')
            
            if not all([ubicacion, zona, pais]):
                return
            
            try:
                temp_max = float(temp_max) if temp_max is not None else None
                temp_min = float(temp_min) if temp_min is not None else None
                precipitacion = float(precipitacion) if precipitacion is not None else 0.0
            except:
                return
            
            clave = f"{ubicacion}_{zona}"
            eventos = []
            
            if temp_max is not None and temp_max > 40:
                eventos.append({'tipo': 'calor_extremo', 'valor': temp_max})
            
            if temp_min is not None and temp_min < 0:
                eventos.append({'tipo': 'frio_extremo', 'valor': temp_min})
            
            if precipitacion >= self.options.precip_extreme_threshold:
                eventos.append({'tipo': 'precipitacion_extrema', 'valor': precipitacion})
            
            if precipitacion == 0.0:
                eventos.append({'tipo': 'sequia', 'valor': 0})
            
            if eventos:
                valor = {
                    'location': ubicacion,
                    'country': pais,
                    'zone': zona,
                    'events': eventos
                }
                yield (clave, json.dumps(valor))
        except:
            pass
    
    def combiner(self, clave, valores):
        eventos_por_tipo = defaultdict(int)
        ubicacion = None
        pais = None
        zona = None
        total_eventos = 0
        
        for valor_json in valores:
            datos = json.loads(valor_json)
            ubicacion = datos['location']
            pais = datos['country']
            zona = datos['zone']
            
            for evento in datos['events']:
                eventos_por_tipo[evento['tipo']] += 1
                total_eventos += 1
        
        agregado = {
            'location': ubicacion,
            'country': pais,
            'zone': zona,
            'events_by_type': dict(eventos_por_tipo),
            'total_events': total_eventos
        }
        
        yield (clave, json.dumps(agregado))
    
    def reducer(self, clave, valores):
        eventos_finales = defaultdict(int)
        ubicacion = None
        pais = None
        zona = None
        total_eventos = 0
        
        for valor_json in valores:
            datos = json.loads(valor_json)
            ubicacion = datos['location']
            pais = datos['country']
            zona = datos['zone']
            total_eventos += datos['total_events']
            
            for tipo, count in datos['events_by_type'].items():
                eventos_finales[tipo] += count
        
        resultado = {
            'ubicacion': ubicacion,
            'zona_climatica': zona,
            'pais': pais,
            'total_eventos': total_eventos,
            'eventos_por_tipo': dict(eventos_finales)
        }
        
        yield (clave, json.dumps(resultado))
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    AnalisisClimaExtremo.run()
