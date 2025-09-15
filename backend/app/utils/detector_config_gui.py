#!/usr/bin/env python3
"""
Simple configuration GUI for the Bottom Signal Detector
Allows users to easily toggle strategies and adjust parameters
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import json
from typing import Dict, Any

class DetectorConfigGUI:
    def __init__(self, initial_config: Dict = None):
        self.root = tk.Tk()
        self.root.title("Bottom Signal Detector Configuration")
        self.root.geometry("800x900")
        
        # Configuration storage
        self.config = initial_config or self.get_default_config()
        self.config_vars = {}
        
        self.setup_ui()
        self.load_config_to_ui()
    
    def get_default_config(self) -> Dict:
        """Default configuration"""
        return {
            # Strategy Toggles
            "enable_quick_bounce": True,
            "enable_support_touch": True,
            "enable_local_low_recovery": True,
            "enable_volume_analysis": True,
            "enable_rsi_oversold": True,
            "enable_price_action": True,
            "enable_market_regime_adjustment": True,
            
            # Strategy Points
            "quick_bounce_points": 30,
            "support_touch_points": 25,
            "local_low_recovery_points": 20,
            "volume_spike_points": 25,
            "volume_high_points": 15,
            "volume_elevated_points": 10,
            "rsi_oversold_points": 15,
            "strong_price_action_points": 15,
            "good_price_action_points": 10,
            "regime_bonus_points": 5,
            
            # Signal Requirements
            "min_signal_score": 50,
            
            # Parameters
            "rsi_threshold": 40,
            "min_bounce_pct": 0.001,
            "support_tolerance": 0.005,
            "volume_spike_threshold": 1.2,
            "price_action_strong_threshold": 70,
            "price_action_good_threshold": 40,
            
            # Advanced Settings
            "enable_market_hours_only": True,
            "enable_daily_loss_limit": True,
            "enable_trade_spacing": True,
            "min_trade_spacing_sec": 60,
            "log_near_misses": True,
            "near_miss_threshold": 30
        }
    
    def setup_ui(self):
        """Setup the GUI interface"""
        # Create notebook for tabs
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Tab 1: Strategy Toggles
        toggles_frame = ttk.Frame(notebook)
        notebook.add(toggles_frame, text="Strategy Toggles")
        self.setup_toggles_tab(toggles_frame)
        
        # Tab 2: Strategy Points
        points_frame = ttk.Frame(notebook)
        notebook.add(points_frame, text="Strategy Points")
        self.setup_points_tab(points_frame)
        
        # Tab 3: Parameters
        params_frame = ttk.Frame(notebook)
        notebook.add(params_frame, text="Parameters")
        self.setup_parameters_tab(params_frame)
        
        # Tab 4: Advanced Settings
        advanced_frame = ttk.Frame(notebook)
        notebook.add(advanced_frame, text="Advanced")
        self.setup_advanced_tab(advanced_frame)
        
        # Bottom buttons
        self.setup_bottom_buttons()
    
    def setup_toggles_tab(self, parent):
        """Setup strategy toggles tab"""
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(main_frame, text="Enable/Disable Strategies", font=('Arial', 12, 'bold')).pack(pady=(0, 10))
        
        toggles = [
            ("enable_quick_bounce", "Quick Bounce Detection", "Detect hammer patterns and quick reversals"),
            ("enable_support_touch", "Support Touch", "Signals when price touches known support levels"),
            ("enable_local_low_recovery", "Local Low Recovery", "New local lows with immediate recovery"),
            ("enable_volume_analysis", "Volume Analysis", "Volume spike detection at lows"),
            ("enable_rsi_oversold", "RSI Oversold", "RSI-based oversold conditions"),
            ("enable_price_action", "Price Action", "Candlestick pattern analysis"),
            ("enable_market_regime_adjustment", "Market Regime", "Adaptive parameters based on market conditions")
        ]
        
        for key, label, description in toggles:
            frame = ttk.Frame(main_frame)
            frame.pack(fill=tk.X, pady=2)
            
            var = tk.BooleanVar()
            self.config_vars[key] = var
            
            cb = ttk.Checkbutton(frame, text=label, variable=var)
            cb.pack(side=tk.LEFT)
            
            ttk.Label(frame, text=f" - {description}", foreground="gray").pack(side=tk.LEFT)
    
    def setup_points_tab(self, parent):
        """Setup strategy points tab"""
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(main_frame, text="Strategy Point Values", font=('Arial', 12, 'bold')).pack(pady=(0, 10))
        ttk.Label(main_frame, text="Higher values make strategies more influential", foreground="gray").pack(pady=(0, 10))
        
        # Create scrollable frame
        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        
        points = [
            ("quick_bounce_points", "Quick Bounce Points", 0, 50),
            ("support_touch_points", "Support Touch Points", 0, 50),
            ("local_low_recovery_points", "Local Low Recovery Points", 0, 50),
            ("volume_spike_points", "Volume Spike Points", 0, 50),
            ("volume_high_points", "Volume High Points", 0, 30),
            ("volume_elevated_points", "Volume Elevated Points", 0, 20),
            ("rsi_oversold_points", "RSI Oversold Points", 0, 30),
            ("strong_price_action_points", "Strong Price Action Points", 0, 30),
            ("good_price_action_points", "Good Price Action Points", 0, 20),
            ("regime_bonus_points", "Market Regime Bonus Points", 0, 15),
            ("min_signal_score", "Minimum Signal Score Required", 20, 100)
        ]
        
        for key, label, min_val, max_val in points:
            frame = ttk.Frame(scrollable_frame)
            frame.pack(fill=tk.X, pady=5)
            
            ttk.Label(frame, text=label, width=30).pack(side=tk.LEFT)
            
            var = tk.IntVar()
            self.config_vars[key] = var
            
            scale = ttk.Scale(frame, from_=min_val, to=max_val, variable=var, orient=tk.HORIZONTAL)
            scale.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(10, 0))
            
            value_label = ttk.Label(frame, text="0", width=5)
            value_label.pack(side=tk.RIGHT)
            
            # Update label when scale changes
            def update_label(val, label=value_label, var=var):
                label.config(text=str(int(float(val))))
            
            scale.config(command=update_label)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
    
    def setup_parameters_tab(self, parent):
        """Setup parameters tab"""
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(main_frame, text="Strategy Parameters", font=('Arial', 12, 'bold')).pack(pady=(0, 10))
        
        params = [
            ("rsi_threshold", "RSI Threshold", 20, 50, "RSI level considered oversold"),
            ("min_bounce_pct", "Minimum Bounce %", 0.0001, 0.01, "Minimum price bounce required"),
            ("support_tolerance", "Support Tolerance", 0.001, 0.02, "Distance tolerance from support levels"),
            ("volume_spike_threshold", "Volume Spike Threshold", 1.0, 3.0, "Multiple of average volume for spike"),
            ("price_action_strong_threshold", "Strong Price Action", 50, 90, "Threshold for strong price action"),
            ("price_action_good_threshold", "Good Price Action", 20, 70, "Threshold for good price action")
        ]
        
        for key, label, min_val, max_val, description in params:
            frame = ttk.Frame(main_frame)
            frame.pack(fill=tk.X, pady=5)
            
            ttk.Label(frame, text=label, width=25).pack(side=tk.LEFT)
            
            var = tk.DoubleVar()
            self.config_vars[key] = var
            
            entry = ttk.Entry(frame, textvariable=var, width=10)
            entry.pack(side=tk.LEFT, padx=(10, 10))
            
            ttk.Label(frame, text=description, foreground="gray").pack(side=tk.LEFT)
    
    def setup_advanced_tab(self, parent):
        """Setup advanced settings tab"""
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        ttk.Label(main_frame, text="Advanced Settings", font=('Arial', 12, 'bold')).pack(pady=(0, 10))
        
        # Boolean settings
        bool_settings = [
            ("enable_market_hours_only", "Trade Only During Market Hours"),
            ("enable_daily_loss_limit", "Enable Daily Loss Limit"),
            ("enable_trade_spacing", "Enable Minimum Trade Spacing"),
            ("log_near_misses", "Log Near-Miss Signals")
        ]
        
        for key, label in bool_settings:
            var = tk.BooleanVar()
            self.config_vars[key] = var
            ttk.Checkbutton(main_frame, text=label, variable=var).pack(anchor=tk.W, pady=2)
        
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        
        # Numeric settings
        numeric_frame = ttk.Frame(main_frame)
        numeric_frame.pack(fill=tk.X)
        
        ttk.Label(numeric_frame, text="Minimum Trade Spacing (seconds):").pack(anchor=tk.W)
        var = tk.IntVar()
        self.config_vars["min_trade_spacing_sec"] = var
        ttk.Entry(numeric_frame, textvariable=var, width=10).pack(anchor=tk.W, pady=(0, 10))
        
        ttk.Label(numeric_frame, text="Near Miss Threshold:").pack(anchor=tk.W)
        var = tk.IntVar()
        self.config_vars["near_miss_threshold"] = var
        ttk.Entry(numeric_frame, textvariable=var, width=10).pack(anchor=tk.W)
    
    def setup_bottom_buttons(self):
        """Setup bottom action buttons"""
        button_frame = ttk.Frame(self.root)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(button_frame, text="Load Preset: Conservative", 
                  command=self.load_conservative_preset).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Load Preset: Aggressive", 
                  command=self.load_aggressive_preset).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Load Preset: Volume Focus", 
                  command=self.load_volume_preset).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(button_frame, text="Load from File", 
                  command=self.load_from_file).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Save to File", 
                  command=self.save_to_file).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Apply Config", 
                  command=self.apply_config).pack(side=tk.RIGHT, padx=5)
    
    def load_config_to_ui(self):
        """Load configuration values into UI"""
        for key, value in self.config.items():
            if key in self.config_vars:
                self.config_vars[key].set(value)
    
    def get_config_from_ui(self) -> Dict:
        """Get configuration from UI values"""
        config = {}
        for key, var in self.config_vars.items():
            config[key] = var.get()
        return config
    
    def load_conservative_preset(self):
        """Load conservative preset"""
        preset = {
            "enable_quick_bounce": True,
            "enable_support_touch": True,
            "enable_local_low_recovery": False,
            "enable_volume_analysis": True,
            "enable_rsi_oversold": True,
            "enable_price_action": True,
            "min_signal_score": 70,
            "rsi_threshold": 35,
            "min_bounce_pct": 0.002
        }
        self.config.update(preset)
        self.load_config_to_ui()
        messagebox.showinfo("Preset Loaded", "Conservative preset applied")
    
    def load_aggressive_preset(self):
        """Load aggressive preset"""
        preset = {
            "enable_quick_bounce": True,
            "enable_support_touch": True,
            "enable_local_low_recovery": True,
            "enable_volume_analysis": True,
            "enable_rsi_oversold": True,
            "enable_price_action": True,
            "min_signal_score": 35,
            "rsi_threshold": 45,
            "min_bounce_pct": 0.0005
        }
        self.config.update(preset)
        self.load_config_to_ui()
        messagebox.showinfo("Preset Loaded", "Aggressive preset applied")
    
    def load_volume_preset(self):
        """Load volume-focused preset"""
        preset = {
            "enable_quick_bounce": False,
            "enable_support_touch": True,
            "enable_local_low_recovery": False,
            "enable_volume_analysis": True,
            "enable_rsi_oversold": False,
            "enable_price_action": True,
            "volume_spike_points": 40,
            "min_signal_score": 50
        }
        self.config.update(preset)
        self.load_config_to_ui()
        messagebox.showinfo("Preset Loaded", "Volume-focused preset applied")
    
    def save_to_file(self):
        """Save configuration to file"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            config = self.get_config_from_ui()
            try:
                with open(filename, 'w') as f:
                    json.dump(config, f, indent=2)
                messagebox.showinfo("Success", f"Configuration saved to {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save: {e}")
    
    def load_from_file(self):
        """Load configuration from file"""
        filename = filedialog.askopenfilename(
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'r') as f:
                    config = json.load(f)
                self.config.update(config)
                self.load_config_to_ui()
                messagebox.showinfo("Success", f"Configuration loaded from {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load: {e}")
    
    def apply_config(self):
        """Apply current configuration"""
        self.config = self.get_config_from_ui()
        messagebox.showinfo("Applied", "Configuration applied successfully!")
        print("Current Configuration:")
        print(json.dumps(self.config, indent=2))
    
    def run(self):
        """Run the GUI"""
        self.root.mainloop()
        return self.config

def main():
    """Run the configuration GUI"""
    gui = DetectorConfigGUI()
    final_config = gui.run()
    
    print("\nFinal Configuration:")
    print(json.dumps(final_config, indent=2))

if __name__ == "__main__":
    main()