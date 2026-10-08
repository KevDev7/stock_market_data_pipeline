"""Regression checks for the editable overview's agreed visual/data-flow contract."""

from pathlib import Path
import re
import unittest
import xml.etree.ElementTree as ET

from scripts.build_architecture_diagram import DATA, HEIGHT, build_diagram


class ArchitectureDiagramTests(unittest.TestCase):
    def setUp(self):
        self.diagram = build_diagram()
        self.boxes = {box.id: box for box in self.diagram.boxes}
        self.edges = {edge['id']: edge for edge in self.diagram.edges}

    def test_three_sources_feed_one_extractor(self):
        source_ids = {'source-prices', 'source-catalog', 'source-overview'}
        source_edges = [edge for edge in self.diagram.edges if edge['source'] in source_ids]
        self.assertEqual({edge['source'] for edge in source_edges}, source_ids)
        self.assertEqual(len(source_edges), 3)
        self.assertTrue(all(edge['target'] == 'extract-job' for edge in source_edges))
        self.assertEqual(sum(box.lines == ('REST API Extraction',)
                             for box in self.diagram.boxes), 1)

    def test_archive_and_copy_are_required_before_raw(self):
        outgoing = {}
        for edge in self.diagram.edges:
            outgoing.setdefault(edge['source'], set()).add(edge['target'])
        self.assertEqual(outgoing['extract-job'], {'s3-archive'})
        self.assertEqual(outgoing['s3-archive'], {'copy-load'})
        self.assertEqual(outgoing['copy-load'],
                         {'raw-stock', 'raw-catalog', 'raw-overview'})

    def test_short_dataset_labels_and_layer_counts(self):
        expected = {
            'RAW': {'raw-stock': 'Stock Prices', 'raw-catalog': 'Ticker Catalog',
                    'raw-overview': 'Company Overview'},
            'STAGING': {'stg-stock': 'Stock Prices', 'stg-catalog': 'Ticker Catalog',
                        'stg-overview': 'Company Overview'},
            'INTERMEDIATE': {'security-reference': 'Security Reference',
                             'price-history': 'Price History',
                             'observed-history': 'Security History'},
            'MARTS': {'analytics-product': 'Data Product Star Schema + Fact Constellation Stock Market Analytics'},
        }
        for layer_name, labels in expected.items():
            layer = self.boxes[f'layer-{layer_name}']
            datasets = {box.id: ' '.join(box.lines) for box in self.diagram.boxes
                        if box.fill == DATA and layer.x <= box.x
                        and box.x + box.w <= layer.x + layer.w
                        and layer.y <= box.y and box.y + box.h <= layer.y + layer.h}
            self.assertEqual(datasets, labels)
        for removed in ('raw-reference', 'stg-constituents', 'membership',
                        'accepted-daily', 'derived-products'):
            self.assertNotIn(removed, self.boxes)

    def test_source_datasets_stay_separate_through_staging(self):
        for suffix in ('stock', 'catalog', 'overview'):
            targets = {edge['target'] for edge in self.diagram.edges
                       if edge['source'] == f'raw-{suffix}'}
            self.assertEqual(targets, {f'stg-{suffix}'})

    def test_joins_and_branches_match_active_model_references(self):
        models = {
            'stg_daily_stocks': 'stg-stock',
            'stg_market__catalog': 'stg-catalog',
            'stg_market__issuer_observations': 'stg-overview',
            'int_market__reference_daily': 'security-reference',
            'int_market__daily': 'price-history',
            'int_market__security_history': 'observed-history',
        }
        model_root = Path(__file__).resolve().parents[1] / 'dbt/stock_analytics/models'
        for model_name, box_id in models.items():
            if model_name.startswith('stg_'):
                continue
            matches = list(model_root.glob(f'**/{model_name}.sql'))
            self.assertEqual(len(matches), 1)
            refs = set(re.findall(r"ref\(['\"]([^'\"]+)['\"]\)",
                                  matches[0].read_text()))
            actual = {edge['source'] for edge in self.diagram.edges
                      if edge['target'] == box_id}
            self.assertEqual(actual, {models[ref] for ref in refs}, model_name)

        mart_files = list((model_root / 'marts').glob('**/*.sql'))
        internal_models = {path.stem for path in mart_files}
        external_refs = {ref for path in mart_files for ref in
                         re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", path.read_text())
                         if ref not in internal_models}
        incoming = {edge['source'] for edge in self.diagram.edges
                    if edge['target'] == 'analytics-product'}
        self.assertEqual(incoming, {models[ref] for ref in external_refs})

    def test_security_history_feeds_marts_directly(self):
        outgoing = {edge['target'] for edge in self.diagram.edges
                    if edge['source'] == 'observed-history'}
        self.assertEqual(outgoing, {'analytics-product'})
        model = (Path(__file__).resolve().parents[1]
                 / 'dbt/stock_analytics/models/marts/dim_security_history.sql')
        self.assertIn("ref('int_market__security_history')", model.read_text())

    def test_one_dashboard_receives_mart_outputs(self):
        dashboard = self.boxes['stock-dashboard']
        self.assertEqual(dashboard.lines, ('Stock Market Dashboard', 'BI Project'))
        incoming = [edge for edge in self.diagram.edges if edge['target'] == dashboard.id]
        self.assertEqual(len(incoming), 1)
        self.assertEqual(incoming[0]['source'], 'analytics-product')
        consumers = [box for box in self.diagram.boxes if 'BI Project' in box.lines]
        self.assertEqual(len(consumers), 1)

    def test_one_mart_product_with_singular_schema_label(self):
        product = self.boxes['analytics-product']
        self.assertEqual(product.lines, ('Data Product', 'Star Schema +',
                                        'Fact Constellation', 'Stock Market', 'Analytics'))
        for removed in ('fact-products', 'dimensions', 'security-history'):
            self.assertNotIn(removed, self.boxes)
        incoming = [edge for edge in self.diagram.edges if edge['target'] == product.id]
        self.assertEqual({edge['source'] for edge in incoming},
                         {'price-history', 'observed-history'})
        self.assertNotIn('layer-MART_STAGING', self.boxes)

    def test_processing_layers_have_equal_widths(self):
        for layer in ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS'):
            self.assertEqual(self.boxes[f'layer-{layer}'].w, 210)
            self.assertEqual(self.boxes[f'header-{layer}'].w, 210)
        self.assertEqual(self.boxes['analytics-product'].w,
                         self.boxes['raw-stock'].w)

    def test_mart_product_matches_dataset_text_padding(self):
        product = self.boxes['analytics-product']
        single_line = self.boxes['raw-stock']
        def text_padding(box):
            return box.h - len(box.lines) * box.font * 1.2
        self.assertAlmostEqual(text_padding(product), text_padding(single_line))
        endpoint = self.edges['shared-attribute-history']['points'][-1]
        self.assertAlmostEqual(endpoint[1], product.y + product.h)
        self.assertTrue(product.x < endpoint[0] < product.x + product.w)

    def test_layer_headers_are_centered_without_icons(self):
        cells = {cell.get('id'): cell
                 for cell in ET.fromstring(self.diagram.drawio()).findall('.//mxCell')}
        for layer in ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS'):
            header_id = f'header-{layer}'
            self.assertFalse(self.boxes[header_id].icon)
            self.assertNotIn(header_id + '-icon', cells)
            style = cells[header_id].get('style')
            self.assertIn('align=center;', style)
            self.assertNotIn('spacingLeft=', style)
            self.assertNotIn('spacingRight=', style)

    def test_lower_sections_are_compact_without_overlap(self):
        for layer in ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS'):
            zone = self.boxes[f'layer-{layer}']
            title = self.boxes[f'policy-title-{layer}']
            self.assertEqual(title.y - (zone.y + zone.h), 24)
        rules_bottom = max(box.y + box.h for box in self.diagram.boxes
                           if box.id.startswith('policy-'))
        self.assertEqual(self.boxes['capability-0'].y - rules_bottom, 20)
        bands_bottom = self.boxes['capability-2'].y + self.boxes['capability-2'].h
        for arrow_id in ('source-orientation', 'business-orientation'):
            arrow = self.boxes[arrow_id]
            self.assertEqual(arrow.y - bands_bottom, 22)
            self.assertLess(arrow.y + arrow.h, HEIGHT)
        self.assertLess(HEIGHT, 1160)

    def test_routes_do_not_cross_other_dataset_boxes(self):
        for edge in self.diagram.edges:
            for box in self.diagram.boxes:
                if box.fill != DATA or box.id in (edge['source'], edge['target']):
                    continue
                for (x1, y1), (x2, y2) in zip(edge['points'], edge['points'][1:]):
                    if y1 == y2:
                        crosses = (box.y < y1 < box.y + box.h
                                   and max(min(x1, x2), box.x)
                                   < min(max(x1, x2), box.x + box.w))
                    else:
                        crosses = (box.x < x1 < box.x + box.w
                                   and max(min(y1, y2), box.y)
                                   < min(max(y1, y2), box.y + box.h))
                    self.assertFalse(crosses, f"{edge['id']} crosses {box.id}")

    def test_simplified_labels_and_removed_annotations(self):
        self.assertEqual([self.boxes[f'capability-{i}'].lines for i in range(3)], [
            ('Metadata & Lineage · dbt',),
            ('Data Quality · dbt Tests',),
            ('Pipeline Orchestration · Airflow',),
        ])
        self.assertEqual(self.boxes['policy-RAW-0'].lines, ('1:1 Copy',))
        self.assertEqual(self.boxes['warehouse-title'].lines, ('Data Warehouse',))
        self.assertEqual(self.boxes['input-contract'].lines,
                         ('DATA CONTRACT', 'Sources → My Platform'))
        self.assertEqual(self.boxes['output-contract'].lines,
                         ('DATA CONTRACT', 'My Platform → Consumers'))
        for removed in ('seeds', 'admin', 'support-label', 'diagram-status',
                        'seed-load', 'app-title', 'usage-note', 'membership-note', 'bi-label'):
            self.assertNotIn(removed, self.boxes)
        self.assertEqual(self.boxes['schedule'].lines,
                         ('Daily Stock Prices +', 'Ticker Catalog:',
                          'Mon–Fri · 12:00 PM ET'))
        self.assertEqual(self.boxes['overview-cadence'].lines,
                         ('Company Overviews:', 'Quarterly + first observed'))

    def test_approved_layer_wide_rules(self):
        expected = {
            'RAW': ['1:1 Copy', 'No Transformations', 'No Dimensional Modeling',
                    'Tables', 'Partial Overwrite'],
            'STAGING': ['Cleanup Transformations', 'Rename', 'Casting', 'Deduplication',
                        'No Cross-Source Joins', 'No Enrichment',
                        'No Dimensional Modeling', 'Views'],
            'INTERMEDIATE': ['Cross-Source Joins', 'Enrichment', 'Shared Business Rules',
                             'No Cleanup',
                             'No Dimensional Modeling', 'Tables', 'Full/Partial Overwrite'],
            'MARTS': ['Analytical Calculations', 'Dimensional Modeling', 'Tables',
                      'No Cleanup', 'Full/Partial Overwrite'],
        }
        for layer, rules in expected.items():
            self.assertEqual(self.boxes[f'policy-title-{layer}'].lines, ('Rules',))
            actual = [box.lines for box in self.diagram.boxes
                      if box.id.startswith(f'policy-{layer}-')]
            self.assertEqual(actual, [(rule,) for rule in rules])

    def test_every_layer_has_one_explicit_object_type(self):
        for layer in ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS'):
            object_types = [box.lines for box in self.diagram.boxes
                            if box.id.startswith(f'policy-{layer}-')
                            and box.lines in (('Tables',), ('Views',))]
            self.assertEqual(object_types,
                             [('Views',)] if layer == 'STAGING' else [('Tables',)])

    def test_every_layer_has_one_explicit_dimensional_modeling_policy(self):
        for layer in ('RAW', 'STAGING', 'INTERMEDIATE', 'MARTS'):
            modeling_rules = [box.lines for box in self.diagram.boxes
                              if box.id.startswith(f'policy-{layer}-')
                              and box.lines in (('Dimensional Modeling',),
                                                ('No Dimensional Modeling',))]
            expected = ('Dimensional Modeling' if layer == 'MARTS'
                        else 'No Dimensional Modeling')
            self.assertEqual(modeling_rules, [(expected,)])

    def test_consumer_access_labels(self):
        for layer in ('RAW', 'STAGING', 'INTERMEDIATE'):
            self.assertEqual(self.boxes[f'usage-{layer}'].lines, ('No Access',))
        self.assertEqual(self.boxes['usage-MARTS'].lines, ('Access',))

    def test_orientation_arrowheads_are_compact(self):
        cells = {cell.get('id'): cell
                 for cell in ET.fromstring(self.diagram.drawio()).findall('.//mxCell')}
        for arrow_id, width in (('source-orientation', 1058),
                                ('business-orientation', 810)):
            cell = cells[arrow_id]
            style = dict(part.split('=', 1) for part in cell.get('style').split(';')
                         if '=' in part)
            self.assertEqual(self.boxes[arrow_id].w, width)
            self.assertAlmostEqual(float(style['arrowSize']) * width, 25)

    def test_drawio_cells_and_connected_endpoints_are_valid(self):
        root = ET.fromstring(self.diagram.drawio())
        cells = root.findall('.//mxCell')
        by_id = {cell.get('id'): cell for cell in cells}
        self.assertEqual(len(cells), len(by_id))
        self.assertIn('0', by_id)
        self.assertIn('1', by_id)
        for edge in self.diagram.edges:
            cell = by_id[edge['id']]
            self.assertEqual(cell.get('edge'), '1')
            for endpoint in ('source', 'target'):
                self.assertIn(cell.get(endpoint), by_id)
            for start, end in zip(edge['points'], edge['points'][1:]):
                self.assertTrue(start[0] == end[0] or start[1] == end[1])
        ET.fromstring(self.diagram.svg())


if __name__ == '__main__':
    unittest.main()
