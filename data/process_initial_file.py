import csv

if __name__ == "__main__":

    list_workclass = ['Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', 'State-gov',
                      'Without-pay', 'Never-worked']
    list_education = ['Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', '9th',
                      '7th-8th', '12th', 'Masters', '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool']
    list_marital_status = ['Married-civ-spouse', 'Divorced', 'Never-married', 'Separated', 'Widowed',
                           'Married-spouse-absent', 'Married-AF-spouse']
    list_occupation = ['Tech-support', 'Craft-repair', 'Other-service', 'Sales', 'Exec-managerial', 'Prof-specialty',
                       'Handlers-cleaners', 'Machine-op-inspct', 'Adm-clerical', 'Farming-fishing', 'Transport-moving',
                       'Priv-house-serv', 'Protective-serv', 'Armed-Forces']
    list_relationship = ['Wife', 'Own-child', 'Husband', 'Not-in-family', 'Other-relative', 'Unmarried']
    list_race = ['White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black']
    list_sex = ['Female', 'Male']
    list_native_country = ['United-States', 'Cambodia', 'England', 'Puerto-Rico', 'Canada', 'Germany',
                           'Outlying-US(Guam-USVI-etc)', 'India', 'Japan', 'Greece', 'South', 'China', 'Cuba', 'Iran',
                           'Honduras', 'Philippines', 'Italy', 'Poland', 'Jamaica', 'Vietnam', 'Mexico', 'Portugal',
                           'Ireland', 'France', 'Dominican-Republic', 'Laos', 'Ecuador', 'Taiwan', 'Haiti', 'Columbia',
                           'Hungary', 'Guatemala', 'Nicaragua', 'Scotland', 'Thailand', 'Yugoslavia', 'El-Salvador',
                           'Trinadad&Tobago', 'Peru', 'Hong', 'Holand-Netherlands']

    dict_workclass = dict(zip(list_workclass, range(0, len(list_workclass))))
    dict_education = dict(zip(list_education, range(0, len(list_education))))
    dict_marital_status = dict(zip(list_marital_status, range(0, len(list_marital_status))))
    dict_occupation = dict(zip(list_occupation, range(0, len(list_occupation))))
    dict_relationship = dict(zip(list_relationship, range(0, len(list_relationship))))
    dict_race = dict(zip(list_race, range(0, len(list_race))))
    dict_sex = dict(zip(list_sex, range(0, len(list_sex))))
    dict_native_country = dict(zip(list_native_country, range(0, len(list_native_country))))
    dict_is_earning_more_than_50 = {
        "<=50K": 0,
        ">50K": 1
    }

    data_without_string = []
    with open("adult.data", 'r', newline='') as csvfile:
        filereaderData = csv.reader(csvfile, delimiter=',')
        for i, row in enumerate(filereaderData):
            for j in range(0, len(row)): row[j] = row[j].lstrip()
            data_without_string.append([row[0], dict_workclass.get(row[1], "null"), row[2],
                                        dict_education.get(row[3], "null"), dict_marital_status.get(row[5], "null"),
                                        dict_occupation.get(row[6], "null"), dict_relationship.get(row[7], "null"),
                                        dict_race.get(row[8], "null"), dict_sex.get(row[9], "null"), row[10], row[11],
                                        row[12], dict_native_country.get(row[13], "null"),
                                        dict_is_earning_more_than_50.get(row[14], "null")])
            print(i)
            '''
            data_without_string[i][0] = row[0]
            data_without_string[i][1] = dict_workclass(row[1])
            data_without_string[i][2] = row[2]
            data_without_string[i][3] = dict_education(row[3])
            # La colonne 4 contient "education-num" que l'on remplace à l'aide de dict_education
            data_without_string[i][4] = dict_marital_status(row[5])
            data_without_string[i][5] = dict_occupation(row[6])
            data_without_string[i][6] = dict_relationship(row[7])
            data_without_string[i][7] = dict_race(row[8])
            data_without_string[i][8] = dict_sex(row[9])
            data_without_string[i][9] = row[10]
            data_without_string[i][10] = row[11]
            data_without_string[i][11] = row[12]
            data_without_string[i][12] = dict_native_country(row[13])
            data_without_string[i][13] = dict_is_earning_more_than_50(row[14])
            '''

    filename = 'adult_processed_data.data'
    header = ['age', 'workclass', 'fnlwgt', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'is-upper-than-50k']
    with open(filename, 'w', newline='') as file:
        csvwriter = csv.writer(file)
        csvwriter.writerow(header)
        csvwriter.writerows(data_without_string)


