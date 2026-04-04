"""
test_hypothesis_generator.py — Tests for HypothesisGenerator
TARE (Tick-Level Algorithmic Research Environment)

Test hypothesis generation, mutation, and determinism.
"""

import unittest
from tare.research.hypothesis_generator import HypothesisGenerator


class TestHypothesisGeneratorBasics(unittest.TestCase):
    """Test basic HypothesisGenerator initialization and template registration."""
    
    def setUp(self) -> None:
        """Create fresh generator for each test."""
        self.gen = HypothesisGenerator()
    
    def test_init(self) -> None:
        """Test generator initialization."""
        self.assertEqual(self.gen.count_templates(), 0)
        self.assertEqual(self.gen.get_registered_templates(), [])
    
    def test_register_template_valid(self) -> None:
        """Test successful template registration."""
        self.gen.register_template(
            'momentum',
            'Momentum strategy',
            {
                'period': (5, 20, 1),
                'threshold': (10, 100, 5)
            }
        )
        
        self.assertEqual(self.gen.count_templates(), 1)
        self.assertIn('momentum', self.gen.get_registered_templates())
    
    def test_register_template_duplicate(self) -> None:
        """Test duplicate template registration raises error."""
        self.gen.register_template(
            'momentum',
            'Strategy 1',
            {'period': (5, 20, 1)}
        )
        
        with self.assertRaises(ValueError) as ctx:
            self.gen.register_template(
                'momentum',
                'Strategy 2',
                {'period': (5, 20, 1)}
            )
        
        self.assertIn('already registered', str(ctx.exception))
    
    def test_register_template_empty_name(self) -> None:
        """Test empty template name raises error."""
        with self.assertRaises(ValueError) as ctx:
            self.gen.register_template(
                '',
                'Description',
                {'param': (0, 10, 1)}
            )
        
        self.assertIn('non-empty string', str(ctx.exception))
    
    def test_register_template_empty_description(self) -> None:
        """Test empty description raises error."""
        with self.assertRaises(ValueError) as ctx:
            self.gen.register_template(
                'momentum',
                '',
                {'param': (0, 10, 1)}
            )
        
        self.assertIn('non-empty string', str(ctx.exception))
    
    def test_register_template_invalid_parameters(self) -> None:
        """Test invalid parameter specification raises error."""
        with self.assertRaises(ValueError):
            self.gen.register_template(
                'momentum',
                'Strategy',
                {
                    'period': (5, 20)  # Missing step
                }
            )
    
    def test_register_template_negative_step(self) -> None:
        """Test negative step value raises error."""
        with self.assertRaises(ValueError) as ctx:
            self.gen.register_template(
                'momentum',
                'Strategy',
                {'period': (5, 20, -1)}
            )
        
        self.assertIn('step must be positive', str(ctx.exception))
    
    def test_register_template_min_greater_than_max(self) -> None:
        """Test min > max raises error."""
        with self.assertRaises(ValueError) as ctx:
            self.gen.register_template(
                'momentum',
                'Strategy',
                {'period': (20, 5, 1)}
            )
        
        self.assertIn('min > max', str(ctx.exception))


class TestHypothesisGeneration(unittest.TestCase):
    """Test hypothesis generation from templates."""
    
    def setUp(self) -> None:
        """Set up generator with test template."""
        self.gen = HypothesisGenerator()
        self.gen.register_template(
            'momentum',
            'Momentum strategy',
            {
                'period': (5, 20, 1),
                'threshold': (10, 100, 5)
            }
        )
    
    def test_generate_basic(self) -> None:
        """Test basic hypothesis generation."""
        hypotheses = self.gen.generate(5, 'momentum', seed=42)
        
        self.assertEqual(len(hypotheses), 5)
        
        for hyp in hypotheses:
            self.assertIn('id', hyp)
            self.assertIn('template', hyp)
            self.assertIn('parameters', hyp)
            self.assertEqual(hyp['template'], 'momentum')
            self.assertIsInstance(hyp['parameters'], dict)
            self.assertIn('period', hyp['parameters'])
            self.assertIn('threshold', hyp['parameters'])
    
    def test_generate_parameter_ranges(self) -> None:
        """Test generated parameters are within registered ranges."""
        hypotheses = self.gen.generate(10, 'momentum', seed=0)
        
        for hyp in hypotheses:
            period = hyp['parameters']['period']
            threshold = hyp['parameters']['threshold']
            
            self.assertGreaterEqual(period, 5)
            self.assertLessEqual(period, 20)
            self.assertGreaterEqual(threshold, 10)
            self.assertLessEqual(threshold, 100)
    
    def test_generate_deterministic(self) -> None:
        """Test same seed produces identical hypotheses."""
        hyp1 = self.gen.generate(3, 'momentum', seed=123)
        hyp2 = self.gen.generate(3, 'momentum', seed=123)
        
        self.assertEqual(hyp1, hyp2)
    
    def test_generate_different_seeds(self) -> None:
        """Test different seeds produce different hypotheses."""
        hyp1 = self.gen.generate(3, 'momentum', seed=1)
        hyp2 = self.gen.generate(3, 'momentum', seed=2)
        
        # At least one hypothesis should differ
        different = False
        for h1, h2 in zip(hyp1, hyp2):
            if h1 != h2:
                different = True
                break
        
        self.assertTrue(different)
    
    def test_generate_invalid_n(self) -> None:
        """Test invalid n value raises error."""
        with self.assertRaises(ValueError):
            self.gen.generate(0, 'momentum')
        
        with self.assertRaises(ValueError):
            self.gen.generate(-5, 'momentum')
    
    def test_generate_invalid_template(self) -> None:
        """Test unknown template raises error."""
        with self.assertRaises(ValueError) as ctx:
            self.gen.generate(5, 'nonexistent')
        
        self.assertIn('not registered', str(ctx.exception))
    
    def test_generate_invalid_seed_type(self) -> None:
        """Test non-int seed raises error."""
        with self.assertRaises(TypeError):
            self.gen.generate(5, 'momentum', seed=3.14)


class TestMutation(unittest.TestCase):
    """Test strategy mutation."""
    
    def setUp(self) -> None:
        """Set up generator with test template."""
        self.gen = HypothesisGenerator()
        self.gen.register_template(
            'momentum',
            'Momentum strategy',
            {
                'period': (5, 20, 1),
                'threshold': (10, 100, 5)
            }
        )
        
        # Generate a base strategy
        hypotheses = self.gen.generate(1, 'momentum', seed=0)
        self.base_strategy = hypotheses[0]
    
    def test_mutate_basic(self) -> None:
        """Test basic mutation."""
        mutant = self.gen.mutate(self.base_strategy, mutation_seed=1)
        
        self.assertIsNotNone(mutant)
        self.assertEqual(mutant['template'], self.base_strategy['template'])
        self.assertIn('parent_id', mutant)
        self.assertEqual(mutant['parent_id'], self.base_strategy['id'])
        self.assertNotEqual(mutant['id'], self.base_strategy['id'])
    
    def test_mutate_deterministic(self) -> None:
        """Test same mutation seed produces identical mutants."""
        mutant1 = self.gen.mutate(self.base_strategy, mutation_seed=42)
        mutant2 = self.gen.mutate(self.base_strategy, mutation_seed=42)
        
        self.assertEqual(mutant1, mutant2)
    
    def test_mutate_different_seeds(self) -> None:
        """Test different mutation seeds produce different mutants."""
        mutant1 = self.gen.mutate(self.base_strategy, mutation_seed=1)
        mutant2 = self.gen.mutate(self.base_strategy, mutation_seed=2)
        
        self.assertNotEqual(mutant1['id'], mutant2['id'])
    
    def test_mutate_invalid_strategy(self) -> None:
        """Test mutation with invalid strategy raises error."""
        with self.assertRaises(ValueError):
            self.gen.mutate({'not': 'valid'})
    
    def test_mutate_invalid_mutation_seed(self) -> None:
        """Test non-int mutation seed raises error."""
        with self.assertRaises(TypeError):
            self.gen.mutate(self.base_strategy, mutation_seed=2.5)


class TestTemplateQueries(unittest.TestCase):
    """Test template query methods."""
    
    def test_get_registered_templates(self) -> None:
        """Test retrieving list of registered templates."""
        gen = HypothesisGenerator()
        
        self.assertEqual(gen.get_registered_templates(), [])
        
        gen.register_template('alpha', 'Alpha', {'x': (0, 10, 1)})
        gen.register_template('beta', 'Beta', {'y': (0, 20, 2)})
        
        templates = gen.get_registered_templates()
        self.assertEqual(len(templates), 2)
        self.assertIn('alpha', templates)
        self.assertIn('beta', templates)
        self.assertEqual(templates, sorted(templates))
    
    def test_get_template_info(self) -> None:
        """Test retrieving template information."""
        gen = HypothesisGenerator()
        gen.register_template(
            'momentum',
            'Momentum strategy',
            {'period': (5, 20, 1), 'threshold': (10, 100, 5)}
        )
        
        info = gen.get_template_info('momentum')
        
        self.assertEqual(info['description'], 'Momentum strategy')
        self.assertIn('period', info['parameters'])
        self.assertIn('threshold', info['parameters'])
        self.assertEqual(info['parameters']['period'], (5, 20, 1))
    
    def test_get_template_info_nonexistent(self) -> None:
        """Test querying nonexistent template returns empty dict."""
        gen = HypothesisGenerator()
        info = gen.get_template_info('nonexistent')
        self.assertEqual(info, {})
    
    def test_count_templates(self) -> None:
        """Test counting registered templates."""
        gen = HypothesisGenerator()
        self.assertEqual(gen.count_templates(), 0)
        
        gen.register_template('t1', 'Template 1', {'x': (0, 10, 1)})
        self.assertEqual(gen.count_templates(), 1)
        
        gen.register_template('t2', 'Template 2', {'y': (0, 20, 1)})
        self.assertEqual(gen.count_templates(), 2)
    
    def test_clear_cache(self) -> None:
        """Test cache clearing doesn't affect templates."""
        gen = HypothesisGenerator()
        gen.register_template('momentum', 'Strategy', {'p': (5, 20, 1)})
        
        gen.generate(5, 'momentum', seed=0)
        self.assertGreater(len(gen._hypothesis_cache), 0)
        
        gen.clear_cache()
        self.assertEqual(len(gen._hypothesis_cache), 0)
        
        # Templates should still be available
        self.assertEqual(gen.count_templates(), 1)


if __name__ == '__main__':
    unittest.main()
