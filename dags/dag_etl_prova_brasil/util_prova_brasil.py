class UtilProvaBrasil():
    
    def get_descricao_serie(self, row):
        return '4ª série/5º ano EF' if row['ID_SERIE'] == 5 else '8ª série/9º ano EF'

    def get_descricao_turno(self, row):
        if row['ID_TURNO'] == 1:
            return 'Matutino'
        elif row['ID_TURNO'] == 2:
            return 'Vespertino'
        elif row['ID_TURNO'] == 3:
            return 'Noturno'
        else:
            return 'Intermediário'
        
    def get_descricao_localizacao(self, row):
        return 'Urbana' if row['ID_LOCALIZACAO'] == 1 else 'Rural'

    def get_descricao_dependencia_adm(self, row):
        if row['ID_DEPENDENCIA_ADM'] == 1:
            return 'Federal'
        elif row['ID_DEPENDENCIA_ADM'] == 2:
            return 'Estadual'
        else:
            return 'Municipal'