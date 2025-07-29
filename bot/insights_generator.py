import logging
import openai
import math
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import config

logger = logging.getLogger(__name__)

class InsightsGenerator:
    def __init__(self):
        if config.openai_api_key:
            self.enabled = True
        else:
            self.enabled = False
            logger.warning("OpenAI API key not configured, insights disabled")

    async def generate_trade_insights(self, strategy_name: str, trades: List[Dict], currency: str, size: float, premium: float, index_price: float) -> Optional[str]:
        """
        Generate insights for options trading strategy using OpenAI API
        
        Args:
            strategy_name: Name of the trading strategy (e.g., "LONG BTC CALL SPREAD")
            trades: List of trade objects with details
            currency: Currency (BTC/ETH)
            size: Position size
            premium: Net premium paid/received
            index_price: Current underlying price
            
        Returns:
            Insights string (max 100 words) or None if disabled/failed
        """
        if not self.enabled:
            return None
            
        try:
            # Build context for the AI
            context = self._build_trade_context(strategy_name, trades, currency, size, premium, index_price)
            
            # Create prompt for insights
            current_date = datetime.now().strftime("%Y-%m-%d")
            prompt = f"""You are an options trading expert analyzing a block trade from the perspective of the trader who initiated it. Today's date is {current_date}. Based on the following trade information, generate a concise (≤100 words) market insight for Telegram users.

{context}

Analysis Framework:
1. Market Directional Bias: What market view does this trade express? (bullish/bearish/neutral with timeframe)
2. Risk Profile: Describe the trader's risk exposure and positioning intent
3. Key Levels: Highlight important strikes and expiries relevant to this position
4. Volatility Positioning: Is the trader long/short volatility? What's the IV context?
5. Market Signal: What does this positioning suggest about market sentiment?

Requirements:
- Analyze from the trader's perspective who initiated this position
- Focus on market view and positioning intent, not calculations
- Use Greeks data for qualitative risk assessment only
- Be specific with timeframes and levels (e.g., "Bullish bias into July expiry")
- Professional tone for fast-paced traders

Avoid:
- Any mathematical calculations or computations
- Counterparty analysis or market maker perspectives
- Generic commentary without trade-specific context
- Vague timeframes ("soon", "in the future")
- Pure trade structure description (assume reader sees the trade)"""

            client = openai.OpenAI(api_key=config.openai_api_key)
            response = client.chat.completions.create(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": "You are an expert options trader providing concise market insights."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.7
            )
            
            insights = response.choices[0].message.content.strip()
            
            # Ensure it's within word limit
            words = insights.split()
            if len(words) > 100:
                insights = ' '.join(words[:100]) + "..."
                
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return None

    def _build_trade_context(self, strategy_name: str, trades: List[Dict], currency: str, size: float, premium: float, index_price: float) -> str:
        """Build context string for AI prompt"""
        
        context_parts = [
            f"Strategy: {strategy_name}",
            f"Asset: {currency}",
            f"Current Price: ${index_price:,.2f}",
            f"Position Size: {size}",
            f"Net Premium: {premium:,.4f} {'₿' if currency=='BTC' else 'Ξ'}"
        ]
        
        # Calculate max gain/loss for the strategy
        max_gain, max_loss = self._get_simplified_max_gain_loss(strategy_name, trades, premium)
        if max_gain is None and max_loss is None:
            max_gain, max_loss = self._calculate_max_gain_loss(strategy_name, trades, index_price, premium)
        
        # Add max gain/loss to context
        if max_gain is not None or max_loss is not None:
            context_parts.append("\n--- Risk/Reward Profile ---")
            if max_gain is not None:
                if abs(max_gain) < 0.0001:
                    context_parts.append("Max Gain: ~0")
                else:
                    context_parts.append(f"Max Gain: {max_gain:,.4f} {'₿' if currency=='BTC' else 'Ξ'}")
            else:
                context_parts.append("Max Gain: Unlimited")
            
            if max_loss is not None:
                if abs(max_loss) < 0.0001:
                    context_parts.append("Max Loss: ~0")
                else:
                    context_parts.append(f"Max Loss: {abs(max_loss):,.4f} {'₿' if currency=='BTC' else 'Ξ'}")
            else:
                context_parts.append("Max Loss: Unlimited")
            
            # Add risk-reward ratio if both are finite
            if max_gain is not None and max_loss is not None and max_loss != 0:
                risk_reward_ratio = abs(max_gain) / abs(max_loss)
                context_parts.append(f"Risk-Reward Ratio: 1:{risk_reward_ratio:.2f}")

        # Calculate valuable metrics from trades data
        if trades and len(trades) > 0:
            # Calculate aggregated metrics
            total_delta = 0
            total_gamma = 0
            total_vega = 0
            total_theta = 0
            avg_iv = 0
            iv_count = 0
            strikes = []
            expiries = []
            moneyness_values = []
            
            trade_legs = []
            for i, trade in enumerate(trades):
                leg_info = []
                
                # Direction (buy/sell)
                if trade.get("direction"):
                    leg_info.append(trade["direction"].upper())
                
                # Call or Put
                call_or_put = None
                if trade.get("callOrPut"):
                    call_or_put = trade["callOrPut"]
                    leg_info.append(call_or_put)
                
                # Strike and moneyness calculation
                if trade.get("strike"):
                    strike = float(trade["strike"])
                    strikes.append(strike)
                    moneyness = strike / index_price
                    moneyness_values.append((moneyness, call_or_put))
                    leg_info.append(f"${strike}")
                    leg_info.append(f"Moneyness: {moneyness:.2f}")
                
                # Expiry
                if trade.get("expiry"):
                    expiry = trade["expiry"]
                    expiries.append(expiry)
                    leg_info.append(expiry)
                
                # Size
                if trade.get("size"):
                    leg_info.append(f"Size: {trade['size']}")
                
                # IV
                if trade.get("iv"):
                    iv = float(trade["iv"])
                    avg_iv += iv
                    iv_count += 1
                    leg_info.append(f"IV: {iv:.1f}%")
                
                # Greeks and aggregate them
                if trade.get("greeks"):
                    greeks = trade["greeks"]
                    greek_parts = []
                    if trade["direction"].upper() == "BUY":
                        trade_size = float(trade.get("size", 0))
                    else:
                        trade_size = -float(trade.get("size", 0))
                    
                    if "delta" in greeks:
                        delta = float(greeks["delta"])
                        total_delta += delta * trade_size
                        greek_parts.append(f"Δ: {delta:.3f}")
                    if "gamma" in greeks:
                        gamma = float(greeks["gamma"])
                        total_gamma += gamma * trade_size
                        greek_parts.append(f"Γ: {gamma:.3f}")
                    if "vega" in greeks:
                        vega = float(greeks["vega"])
                        total_vega += vega * trade_size
                        greek_parts.append(f"ν: {vega:.3f}")
                    if "theta" in greeks:
                        theta = float(greeks["theta"])
                        total_theta += theta * trade_size
                        greek_parts.append(f"Θ: {theta:.3f}")
                    if "rho" in greeks:
                        rho = float(greeks["rho"])
                        greek_parts.append(f"ρ: {rho:.3f}")
                    
                    if greek_parts:
                        leg_info.append(f"Greeks: {', '.join(greek_parts)}")
                
                if leg_info:
                    trade_legs.append(f"Leg {i+1}: {' '.join(leg_info)}")
            
            # Add calculated metrics to context
            context_parts.append("\n--- Calculated Metrics ---")
            
            # Portfolio Greeks
            if total_delta != 0:
                context_parts.append(f"Total Delta Exposure: {total_delta:.2f}")
            if total_gamma != 0:
                context_parts.append(f"Total Gamma Exposure: {total_gamma:.4f}")
            if total_vega != 0:
                context_parts.append(f"Total Vega Exposure: {total_vega:.2f}")
            if total_theta != 0:
                context_parts.append(f"Total Theta Exposure: {total_theta:.2f}")
            
            # IV Analysis
            if iv_count > 0:
                avg_iv = avg_iv / iv_count
                context_parts.append(f"Average IV: {avg_iv:.1f}%")
            
            # Strike Analysis
            if strikes:
                min_strike = min(strikes)
                max_strike = max(strikes)
                context_parts.append(f"Strike Range: ${min_strike} - ${max_strike}")
                
                # Moneyness analysis
                if moneyness_values:
                    avg_moneyness = sum(m[0] for m in moneyness_values) / len(moneyness_values)
                    context_parts.append(f"Average Moneyness: {avg_moneyness:.2f}")
                    
                    # Categorize position based on option type
                    position_types = []
                    for moneyness, option_type in moneyness_values:
                        if option_type == "C":  # Call option
                            if moneyness > 1.05:
                                position_types.append("Deep OTM")
                            elif moneyness > 1.02:
                                position_types.append("OTM")
                            elif moneyness > 0.98:
                                position_types.append("ATM")
                            elif moneyness > 0.95:
                                position_types.append("ITM")
                            else:
                                position_types.append("Deep ITM")
                        elif option_type == "P":  # Put option
                            if moneyness < 0.95:
                                position_types.append("Deep OTM")
                            elif moneyness < 0.98:
                                position_types.append("OTM")
                            elif moneyness < 1.02:
                                position_types.append("ATM")
                            elif moneyness < 1.05:
                                position_types.append("ITM")
                            else:
                                position_types.append("Deep ITM")
                    
                    if position_types:
                        # If all legs have same classification, show it; otherwise show mixed
                        unique_positions = list(set(position_types))
                        if len(unique_positions) == 1:
                            context_parts.append(f"Position: {unique_positions[0]}")
                        else:
                            context_parts.append(f"Position: Mixed ({'/'.join(unique_positions)})")
            
            # Expiry Analysis
            if expiries:
                unique_expiries = list(set(expiries))
                context_parts.append(f"Expiries: {', '.join(unique_expiries)}")
                
                # Calculate DTE (Days to Expiry) for closest expiry
                try:
                    from datetime import datetime
                    closest_expiry = min(unique_expiries)
                    expiry_date = datetime.strptime(closest_expiry, "%d%b%y")
                    current_date = datetime.now()
                    dte = (expiry_date - current_date).days
                    context_parts.append(f"Days to Closest Expiry: {dte}")
                    
                    # Time decay classification
                    if dte <= 7:
                        context_parts.append("Time Decay: High (weekly expiry)")
                    elif dte <= 30:
                        context_parts.append("Time Decay: Medium (monthly expiry)")
                    else:
                        context_parts.append("Time Decay: Low (longer-term)")
                except:
                    pass
            
            # Premium Analysis
            if premium != 0:
                premium_pct = premium * 100
                context_parts.append(f"Premium as % of Spot: {premium_pct:.3f}%")
                
                if premium > 0:
                    context_parts.append("Premium Flow: Net Debit (paid premium)")
                else:
                    context_parts.append("Premium Flow: Net Credit (received premium)")
            
            # Add trade legs
            if trade_legs:
                context_parts.append("\n--- Trade Legs ---")
                context_parts.extend(trade_legs)
        
        return "\n".join(context_parts)

    def _calculate_max_gain_loss(self, strategy_name: str, trades: List[Dict], index_price: float, premium: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate maximum gain and maximum loss for options strategies.
        
        Args:
            strategy_name: Name of the trading strategy
            trades: List of trade objects with details
            index_price: Current underlying price
            premium: Net premium paid/received
            
        Returns:
            Tuple of (max_gain, max_loss) in currency units, None if cannot calculate
        """
        if not trades or len(trades) == 0:
            return None, None
            
        try:
            # Get strikes and option types
            legs = []
            for trade in trades:
                if not all(key in trade and trade[key] is not None for key in ['strike', 'callOrPut', 'direction', 'size']):
                    continue
                    
                legs.append({
                    'strike': float(trade['strike']),
                    'option_type': trade['callOrPut'],  # 'C' or 'P'
                    'direction': trade['direction'].upper(),  # 'BUY' or 'SELL'
                    'size': abs(float(trade['size'])),
                    'multiplier': 1 if trade['direction'].upper() == 'BUY' else -1
                })
            
            if not legs:
                return None, None
            
            # Check if strategy has unlimited profit/loss potential
            has_unlimited_gain = False
            has_unlimited_loss = False
            
            # Analyze legs for unlimited potential
            net_call_multiplier = 0
            net_put_multiplier = 0
            
            for leg in legs:
                if leg['option_type'] == 'C':
                    net_call_multiplier += leg['multiplier'] * leg['size']
                else:
                    net_put_multiplier += leg['multiplier'] * leg['size']
            
            # Long calls (net positive call exposure) have unlimited upside
            if net_call_multiplier > 0:
                has_unlimited_gain = True
            # Short calls (net negative call exposure) have unlimited downside  
            elif net_call_multiplier < 0:
                has_unlimited_loss = True
            
            # Long puts have limited gain (strike - premium), not unlimited
            # Short puts have limited loss (strike + premium), not unlimited
            
            if has_unlimited_gain or has_unlimited_loss:
                # For unlimited strategies, still calculate breakeven and limited side
                min_strike = min(leg['strike'] for leg in legs)
                max_strike = max(leg['strike'] for leg in legs)
                
                # Calculate limited profit/loss at key points
                test_prices = [0.01, min_strike * 0.5, min_strike, (min_strike + max_strike) / 2, max_strike, max_strike * 1.5]
                payoffs = []
                
                for spot_price in test_prices:
                    total_payoff = -premium
                    for leg in legs:
                        if leg['option_type'] == 'C':
                            intrinsic = max(0, spot_price - leg['strike'])
                        else:
                            intrinsic = max(0, leg['strike'] - spot_price)
                        leg_payoff = leg['multiplier'] * leg['size'] * intrinsic
                        total_payoff += leg_payoff
                    payoffs.append(total_payoff)
                
                if has_unlimited_gain:
                    max_gain = None  # Unlimited
                    max_loss = min(payoffs)
                else:  # has_unlimited_loss
                    max_gain = max(payoffs)
                    max_loss = None  # Unlimited
            else:
                # For strategies with limited profit/loss, use comprehensive range
                min_strike = min(leg['strike'] for leg in legs)
                max_strike = max(leg['strike'] for leg in legs)
                
                price_range = []
                step = (max_strike - min_strike) / 50 if max_strike > min_strike else max_strike * 0.02
                start_price = max(0.01, min_strike - max_strike * 0.2)
                end_price = max_strike * 1.5
                
                current = start_price
                while current <= end_price:
                    price_range.append(current)
                    current += step
                
                payoffs = []
                for spot_price in price_range:
                    total_payoff = -premium
                    for leg in legs:
                        if leg['option_type'] == 'C':
                            intrinsic = max(0, spot_price - leg['strike'])
                        else:
                            intrinsic = max(0, leg['strike'] - spot_price)
                        leg_payoff = leg['multiplier'] * leg['size'] * intrinsic
                        total_payoff += leg_payoff
                    payoffs.append(total_payoff)
                
                max_gain = max(payoffs) if payoffs else None
                max_loss = min(payoffs) if payoffs else None
                
            return max_gain, max_loss
            
        except Exception as e:
            logger.error(f"Error calculating max gain/loss: {e}")
            return None, None

    def _get_simplified_max_gain_loss(self, strategy_name: str, trades: List[Dict], premium: float) -> Tuple[Optional[float], Optional[float]]:
        """
        Get simplified max gain/loss for common strategies without complex calculations.
        """
        strategy_upper = strategy_name.upper()
        
        try:
            # Single leg strategies
            if "LONG CALL" in strategy_upper and "SPREAD" not in strategy_upper:
                return None, abs(premium)  # Unlimited upside, limited downside (premium paid)
            
            elif "SHORT CALL" in strategy_upper and "SPREAD" not in strategy_upper:
                return abs(premium), None  # Limited upside (premium received), unlimited downside
            
            elif "LONG PUT" in strategy_upper and "SPREAD" not in strategy_upper:
                if trades and len(trades) > 0 and trades[0].get('strike'):
                    # Max gain when underlying goes to 0: strike - premium_paid
                    max_gain = float(trades[0]['strike']) - abs(premium)
                    # Max loss is premium paid
                    return max_gain, abs(premium)
                return None, abs(premium)
            
            elif "SHORT PUT" in strategy_upper and "SPREAD" not in strategy_upper:
                if trades and len(trades) > 0 and trades[0].get('strike'):
                    # Max loss when underlying goes to 0: strike - premium_received
                    max_loss = float(trades[0]['strike']) - abs(premium)
                    # Max gain is premium received
                    return abs(premium), max_loss
                return abs(premium), None
            
            # Spread strategies
            elif "SPREAD" in strategy_upper and len(trades) >= 2:
                strikes = [float(trade['strike']) for trade in trades if trade.get('strike')]
                if len(strikes) >= 2:
                    spread_width = abs(max(strikes) - min(strikes))
                    
                    if "CALL SPREAD" in strategy_upper or "PUT SPREAD" in strategy_upper:
                        if premium > 0:  # Debit spread
                            max_gain = spread_width - abs(premium)
                            max_loss = abs(premium)
                        else:  # Credit spread
                            max_gain = abs(premium)
                            max_loss = spread_width - abs(premium)
                        return max_gain, max_loss
            
            # Straddle strategies
            elif "STRADDLE" in strategy_upper and len(trades) >= 2:
                if "LONG" in strategy_upper:
                    # Long straddle: unlimited gain, limited loss (premium paid)
                    return None, abs(premium)
                elif "SHORT" in strategy_upper:
                    # Short straddle: limited gain (premium received), unlimited loss
                    return abs(premium), None
            
            # Strangle strategies  
            elif "STRANGLE" in strategy_upper and len(trades) >= 2:
                if "LONG" in strategy_upper:
                    # Long strangle: unlimited gain, limited loss (premium paid)
                    return None, abs(premium)
                elif "SHORT" in strategy_upper:
                    # Short strangle: limited gain (premium received), unlimited loss
                    return abs(premium), None
            
            # Butterfly strategies
            elif "BUTTERFLY" in strategy_upper and len(trades) >= 3:
                strikes = [float(trade['strike']) for trade in trades if trade.get('strike')]
                if len(strikes) >= 3:
                    strikes.sort()
                    if len(strikes) == 3:
                        # Standard butterfly: max gain at middle strike
                        wing_width = min(strikes[1] - strikes[0], strikes[2] - strikes[1])
                        if premium > 0:  # Long butterfly (debit)
                            max_gain = wing_width - abs(premium)
                            max_loss = abs(premium)
                        else:  # Short butterfly (credit)
                            max_gain = abs(premium)
                            max_loss = wing_width - abs(premium)
                        return max_gain, max_loss
            
            # Condor strategies
            elif "CONDOR" in strategy_upper and len(trades) >= 4:
                strikes = [float(trade['strike']) for trade in trades if trade.get('strike')]
                if len(strikes) >= 4:
                    strikes.sort()
                    # Condor spread width
                    spread_width = min(strikes[1] - strikes[0], strikes[3] - strikes[2])
                    if premium > 0:  # Long condor (debit)
                        max_gain = spread_width - abs(premium)
                        max_loss = abs(premium)
                    else:  # Short condor (credit)
                        max_gain = abs(premium)
                        max_loss = spread_width - abs(premium)
                    return max_gain, max_loss
            
            # Iron Condor strategies
            elif "IRON CONDOR" in strategy_upper and len(trades) >= 4:
                strikes = [float(trade['strike']) for trade in trades if trade.get('strike')]
                if len(strikes) >= 4:
                    strikes.sort()
                    # Iron condor: credit strategy
                    spread_width = min(strikes[1] - strikes[0], strikes[3] - strikes[2])
                    max_gain = abs(premium)  # Credit received
                    max_loss = spread_width - abs(premium)
                    return max_gain, max_loss
            
            # Collar strategies
            elif "COLLAR" in strategy_upper and len(trades) >= 2:
                # Protective collar: limited gain and loss
                call_strikes = [float(t['strike']) for t in trades if t.get('strike') and t.get('callOrPut') == 'C']
                put_strikes = [float(t['strike']) for t in trades if t.get('strike') and t.get('callOrPut') == 'P']
                
                if call_strikes and put_strikes:
                    max_call_strike = max(call_strikes)
                    max_put_strike = max(put_strikes)
                    
                    # Assuming underlying position exists
                    if max_call_strike > max_put_strike:
                        collar_width = max_call_strike - max_put_strike
                        max_gain = collar_width - abs(premium)
                        max_loss = abs(premium)
                        return max_gain, max_loss
            
            # Risk Reversal strategies
            elif "RISK REVERSAL" in strategy_upper or "REVERSAL" in strategy_upper:
                if len(trades) >= 2:
                    # Risk reversal has unlimited gain/loss depending on direction
                    call_trades = [t for t in trades if t.get('callOrPut') == 'C']
                    put_trades = [t for t in trades if t.get('callOrPut') == 'P']
                    
                    if call_trades and put_trades:
                        call_direction = call_trades[0].get('direction', '').upper()
                        put_direction = put_trades[0].get('direction', '').upper()
                        
                        # Long call + Short put = Bullish risk reversal (unlimited gain, unlimited loss)
                        if call_direction == 'BUY' and put_direction == 'SELL':
                            return None, None  # Both unlimited
                        # Short call + Long put = Bearish risk reversal (unlimited loss, unlimited gain)  
                        elif call_direction == 'SELL' and put_direction == 'BUY':
                            return None, None  # Both unlimited
            
            # Calendar/Time spreads
            elif "CALENDAR" in strategy_upper and len(trades) >= 2:
                # Calendar spreads have limited max gain/loss
                # Max gain typically occurs when short option expires worthless
                max_gain = abs(premium) * 2  # Rough estimate
                max_loss = abs(premium)
                return max_gain, max_loss
            
            # Ratio spreads
            elif "RATIO" in strategy_upper and len(trades) >= 2:
                if "CALL" in strategy_upper:
                    # Call ratio spread can have unlimited loss on upside
                    if premium < 0:  # Credit received
                        return abs(premium), None  # Limited gain, unlimited loss
                    else:  # Debit paid
                        strikes = [float(t['strike']) for t in trades if t.get('strike')]
                        if len(strikes) >= 2:
                            spread_width = abs(max(strikes) - min(strikes))
                            max_gain = spread_width - abs(premium)
                            return max_gain, None  # Limited gain, unlimited loss
                elif "PUT" in strategy_upper:
                    # Put ratio spread can have unlimited loss on downside
                    if premium < 0:  # Credit received
                        return abs(premium), None  # Limited gain, unlimited loss
                    else:  # Debit paid
                        strikes = [float(t['strike']) for t in trades if t.get('strike')]
                        if len(strikes) >= 2:
                            spread_width = abs(max(strikes) - min(strikes))
                            max_gain = spread_width - abs(premium)
                            return max_gain, None  # Limited gain, unlimited loss
            
            # For complex strategies, use the full calculation
            else:
                return None, None
                
        except Exception as e:
            logger.error(f"Error in simplified max gain/loss calculation: {e}")
            return None, None
        
        return None, None

# Global instance
insights_generator = InsightsGenerator()
