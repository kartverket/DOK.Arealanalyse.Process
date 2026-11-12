from dokanalyse.caching import dok_status, guidance_data, kartgrunnlag, kartkatalog, codelist


def test_dok_status_mapping():
    data = {'containeditems': [{
        'MetadataUrl': 'https://kartkatalog.geonorge.no/metadata/abc-123',
        'theme': 'Natur',
        'Suitability': {'BuildingMatter': 1, 'ZoningPlan': 4, 'Other': 2}
    }]}

    result = dok_status._map_data(data)

    assert result[0]['dataset_id'] == 'abc-123'
    assert {s['quality_dimension_id'] for s in result[0]['suitability']} == {
        'egnethet_byggesak', 'egnethet_reguleringsplan'}


def test_guidance_data_mapping():
    data = [{
        'id': 'g1', 'tittel': 'Tittel', 'forklarendeTekst': 'Forklaring', 'dialogtekst': 'Dialog',
        'lenker': [{'href': 'https://example.com', 'tittel': 'Lenke'}],
        'muligeTiltak': '- Tiltak 1\n- Tiltak 2\n\n'
    }]

    result = guidance_data._map_data(data)

    assert result[0]['title'] == 'Tittel'
    assert result[0]['guidance_uri'] == [{'href': 'https://example.com', 'title': 'Lenke'}]
    assert result[0]['possible_actions'] == ['Tiltak 1', 'Tiltak 2']


def test_kartgrunnlag_mapping():
    data = {'containeditems': [
        {'ConfirmedDok': 'JA', 'MetadataUrl': 'https://x/metadata/id-1'},
        {'ConfirmedDok': 'NEI', 'MetadataUrl': 'https://x/metadata/id-2'}
    ]}

    assert kartgrunnlag._map_data(data) == ['id-1']


def test_kartkatalog_mapping():
    data = {'NorwegianTitle': 'Flomsoner', 'Abstract': 'A', 'ContactOwner': {'Organization': 'NVE'},
            'DateUpdated': '2024-01-01T00:00:00'}

    result = kartkatalog._map_data(data, 'id-1')

    assert result['title'] == 'Flomsoner'
    assert result['owner'] == 'NVE'
    assert result['datasetDescriptionUri'].endswith('/id-1')


def test_codelist_mapping_only_valid_entries():
    data = {'containeditems': [
        {'status': 'Gyldig', 'codevalue': 'a', 'label': 'A', 'description': ''},
        {'status': 'Utgått', 'codevalue': 'b', 'label': 'B', 'description': ''}
    ]}

    assert [entry['value'] for entry in codelist._map_data(data)] == ['a']
