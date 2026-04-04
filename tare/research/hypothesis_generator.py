"""
hypothesis_generator.py — HypothesisGenerator v1
TARE (Tick-Level Algorithmic Research Environment)

Auto-generate trading hypotheses and strategy variations deterministically.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import List, Dict, Any, Optional
from hashlib import sha256


class HypothesisGenerator:
    """
    Generate trading hypotheses and strategy variations deterministically.
    
    Creates parameterized strategy templates with variations based on
    hash-based deterministic derivations. All operations use integer
    arithmetic only and produce reproducible outputs.
    """
    
    def __init__(self) -> None:
        """
        Initialize HypothesisGenerator with empty state.
        
        Creates internal structures for template storage and variation
        tracking. All initialization is deterministic.
        """
        self._templates: Dict[str, Dict[str, Any]] = {}
        self._hypothesis_cache: Dict[int, List[Dict[str, Any]]] = {}
    
    def register_template(
        self,
        name: str,
        description: str,
        parameters: Dict[str, tuple]
    ) -> None:
        """
        Register strategy template with parameter specifications.
        
        Template defines parameter names and ranges for hypothesis generation.
        All ranges are integer-based for deterministic derivation.
        
        Args:
            name: Template identifier (non-empty string).
            description: Template description (non-empty string).
            parameters: Dict mapping parameter name to (min, max, step) tuple.
                       All values must be integers. Step must be positive.
        
        Raises:
            ValueError: If name/description empty, parameters invalid,
                       or name already registered.
            TypeError: If parameters values are not integers.
        
        Example:
            >>> gen = HypothesisGenerator()
            >>> gen.register_template(
            ...     'momentum',
            ...     'Momentum trading strategy',
            ...     {
            ...         'period': (5, 20, 1),
            ...         'threshold': (10, 100, 5)
            ...     }
            ... )
        """
        if not isinstance(name, str) or not name:
            raise ValueError("name must be non-empty string")
        
        if name in self._templates:
            raise ValueError(f"template '{name}' already registered")
        
        if not isinstance(description, str) or not description:
            raise ValueError("description must be non-empty string")
        
        if not isinstance(parameters, dict):
            raise TypeError("parameters must be dict")
        
        # Validate all parameter ranges
        for param_name, param_range in parameters.items():
            if not isinstance(param_name, str) or not param_name:
                raise ValueError("parameter names must be non-empty strings")
            
            if not isinstance(param_range, (tuple, list)) or len(param_range) != 3:
                raise ValueError(
                    f"parameter '{param_name}' range must be (min, max, step) tuple"
                )
            
            min_val, max_val, step_val = param_range
            
            if not isinstance(min_val, int):
                raise TypeError(f"parameter '{param_name}' min must be int")
            if not isinstance(max_val, int):
                raise TypeError(f"parameter '{param_name}' max must be int")
            if not isinstance(step_val, int):
                raise TypeError(f"parameter '{param_name}' step must be int")
            
            if step_val <= 0:
                raise ValueError(f"parameter '{param_name}' step must be positive")
            
            if min_val > max_val:
                raise ValueError(
                    f"parameter '{param_name}' min > max"
                )
        
        self._templates[name] = {
            'description': description,
            'parameters': parameters.copy()
        }
    
    def _hash_to_int(self, data: str, modulo: int) -> int:
        """
        Convert string to deterministic integer using SHA256.
        
        Uses cryptographic hash for deterministic pseudo-random mapping
        of strings to integers within range [0, modulo).
        
        Args:
            data: Input string to hash.
            modulo: Range for output [0, modulo).
        
        Returns:
            int: Hash-derived integer in range [0, modulo).
        
        Raises:
            ValueError: If modulo <= 0.
        """
        if modulo <= 0:
            raise ValueError("modulo must be positive")
        
        hash_digest = sha256(data.encode('utf-8')).digest()
        hash_int = int.from_bytes(hash_digest[:8], byteorder='big')
        return hash_int % modulo
    
    def _derive_parameter_value(
        self,
        template_name: str,
        param_name: str,
        seed: int,
        variation_index: int
    ) -> int:
        """
        Derive parameter value deterministically from seed and index.
        
        Uses hash-based derivation to map (template, param, seed, index)
        to valid parameter value within registered range.
        
        Args:
            template_name: Template identifier.
            param_name: Parameter name.
            seed: Base seed for derivation (integer).
            variation_index: Variation sequence index (non-negative integer).
        
        Returns:
            int: Valid parameter value in registered range.
        
        Raises:
            ValueError: If template/parameter not found.
        """
        if template_name not in self._templates:
            raise ValueError(f"template '{template_name}' not registered")
        
        template = self._templates[template_name]
        if param_name not in template['parameters']:
            raise ValueError(
                f"parameter '{param_name}' not in template '{template_name}'"
            )
        
        min_val, max_val, step_val = template['parameters'][param_name]
        
        # Create deterministic seed string
        seed_str = f"{template_name}:{param_name}:{seed}:{variation_index}"
        
        # Map to number of steps in range
        num_steps = (max_val - min_val) // step_val + 1
        step_index = self._hash_to_int(seed_str, num_steps)
        
        # Convert step index to actual parameter value
        value = min_val + (step_index * step_val)
        
        return value
    
    def generate(
        self,
        n: int,
        template: str,
        seed: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Generate n hypotheses from template deterministically.
        
        Creates list of n strategy hypotheses by deriving parameter
        values deterministically from template and seed. Same inputs
        always produce identical results.
        
        Args:
            n: Number of hypotheses to generate (positive integer).
            template: Template name (must be registered).
            seed: Deterministic seed for derivation (integer, default 0).
        
        Returns:
            list: List of n hypothesis dicts, each containing:
                  - 'id': Unique hypothesis identifier (integer)
                  - 'template': Template name (string)
                  - 'parameters': Parameter value dict (all integers)
                  - 'seed': Generation seed used
                  - 'index': Position in generation sequence
        
        Raises:
            ValueError: If n <= 0, template not registered.
            TypeError: If seed not integer.
        
        Example:
            >>> gen = HypothesisGenerator()
            >>> gen.register_template('momentum', 'desc', {'period': (5, 20, 1)})
            >>> hypotheses = gen.generate(3, 'momentum', seed=42)
            >>> len(hypotheses)
            3
            >>> hypotheses[0]['template']
            'momentum'
        """
        if not isinstance(n, int) or n <= 0:
            raise ValueError("n must be positive integer")
        
        if template not in self._templates:
            raise ValueError(f"template '{template}' not registered")
        
        if not isinstance(seed, int):
            raise TypeError("seed must be integer")
        
        # Check cache
        cache_key = (n, template, seed)
        cache_hash = hash(cache_key)
        if cache_hash in self._hypothesis_cache:
            return self._hypothesis_cache[cache_hash]
        
        hypotheses: List[Dict[str, Any]] = []
        template_def = self._templates[template]
        parameter_names = list(template_def['parameters'].keys())
        
        for variation_idx in range(n):
            hypothesis_id = self._hash_to_int(
                f"{template}:{seed}:{variation_idx}",
                1000000
            )
            
            # Derive all parameters for this hypothesis
            parameters: Dict[str, int] = {}
            for param_name in parameter_names:
                param_value = self._derive_parameter_value(
                    template,
                    param_name,
                    seed,
                    variation_idx
                )
                parameters[param_name] = param_value
            
            hypothesis: Dict[str, Any] = {
                'id': hypothesis_id,
                'template': template,
                'parameters': parameters,
                'seed': seed,
                'index': variation_idx
            }
            
            hypotheses.append(hypothesis)
        
        # Cache result
        self._hypothesis_cache[cache_hash] = hypotheses
        
        return hypotheses
    
    def mutate(
        self,
        strategy: Dict[str, Any],
        mutation_seed: int = 1
    ) -> Dict[str, Any]:
        """
        Create mutated copy of strategy with altered parameters.
        
        Generates single variation by deterministically modifying one or more
        parameters. Mutation is fully deterministic based on mutation_seed.
        Original strategy unchanged.
        
        Args:
            strategy: Strategy dict with 'template' and 'parameters' keys.
            mutation_seed: Seed for mutation derivation (integer, default 1).
        
        Returns:
            dict: New strategy dict with:
                  - 'id': New unique identifier
                  - 'template': Same as original
                  - 'parameters': Modified parameter values
                  - 'seed': Mutation seed used
                  - 'parent_id': ID of original strategy
                  - 'index': Always 0 (single mutation)
        
        Raises:
            ValueError: If strategy malformed or template not found.
            TypeError: If mutation_seed not integer.
        
        Example:
            >>> strategy = {'id': 123, 'template': 'momentum',
            ...            'parameters': {'period': 10, 'threshold': 50}}
            >>> mutant = gen.mutate(strategy, mutation_seed=42)
            >>> mutant['parent_id']
            123
        """
        if not isinstance(strategy, dict):
            raise TypeError("strategy must be dict")
        
        if 'template' not in strategy or 'parameters' not in strategy:
            raise ValueError("strategy must have 'template' and 'parameters'")
        
        if not isinstance(mutation_seed, int):
            raise TypeError("mutation_seed must be integer")
        
        template_name = strategy['template']
        original_params = strategy['parameters']
        
        if template_name not in self._templates:
            raise ValueError(f"template '{template_name}' not registered")
        
        template_def = self._templates[template_name]
        parameter_names = list(template_def['parameters'].keys())
        
        # Determine mutation magnitude
        num_params = len(parameter_names)
        mutation_magnitude = (mutation_seed % max(1, num_params)) + 1
        
        # Determine which parameters to mutate
        mutated_params: Dict[str, int] = original_params.copy()
        for i in range(mutation_magnitude):
            param_idx = (mutation_seed + i) % num_params
            param_name = parameter_names[param_idx]
            
            # Derive new value using mutation seed
            new_value = self._derive_parameter_value(
                template_name,
                param_name,
                mutation_seed,
                i
            )
            mutated_params[param_name] = new_value
        
        # Create mutated strategy
        parent_id = strategy.get('id', 0)
        mutant_id = self._hash_to_int(
            f"{template_name}:{parent_id}:{mutation_seed}",
            1000000
        )
        
        mutant: Dict[str, Any] = {
            'id': mutant_id,
            'template': template_name,
            'parameters': mutated_params,
            'seed': mutation_seed,
            'parent_id': parent_id,
            'index': 0
        }
        
        return mutant
    
    def get_registered_templates(self) -> List[str]:
        """
        List all registered template names.
        
        Returns:
            list: Sorted list of template identifiers.
        """
        return sorted(self._templates.keys())
    
    def get_template_info(self, name: str) -> Dict[str, Any]:
        """
        Retrieve template specification.
        
        Args:
            name: Template identifier.
        
        Returns:
            dict: Template metadata (description, parameters), or empty dict
                  if template not found.
        """
        if name not in self._templates:
            return {}
        
        template = self._templates[name]
        return {
            'description': template['description'],
            'parameters': template['parameters'].copy()
        }
    
    def count_templates(self) -> int:
        """
        Count registered templates.
        
        Returns:
            int: Number of registered templates.
        """
        return len(self._templates)
    
    def clear_cache(self) -> None:
        """
        Clear hypothesis generation cache.
        
        Removes cached results from previous generate() calls.
        Does not affect registered templates.
        """
        self._hypothesis_cache.clear()
