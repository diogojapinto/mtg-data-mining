"""Module responsible for fetching and caching data from 17Lands API."""

from datetime import date, datetime, timedelta
import json
from typing import Tuple

from box import Box
import pandas as pd
from requests_cache import CachedSession

from utils.api_clients.seventeen_lands.constants import PREMIER_DRAFT


class SeventeenLandsClient:
    """
    17Lands client.

    Uses caching for the API responses, and returns `pandas` objects for most scenarios.
    """

    cache_folder = './data/17lands'

    def __init__(self) -> None:
        # Cache API responses with requests_cache
        self.session = CachedSession(
            self.cache_folder,
            backend='filesystem',
            serializer='json',
            expire_after=timedelta(days=180),
            cache_control=True,
            allowable_codes=[200],
            allowable_method=['GET'],
            stale_if_error=False
        )


    def get_colors(self) -> pd.Series:
        """
        Retrieves all supported color combinations.

        Returns
        -------
        colors : pandas.Series
            A list of all supported color combinations.
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/colors'
        response = self.session.get(url=url).json()

        colors = pd.Series(response)
        return colors


    def get_expansions(self) -> pd.Series:
        """
        Retrieves all supported sets/expansions.

        Returns
        -------
        expansions : pandas.Series
            A series of all supported sets/expansions.
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/expansions'
        response = self.session.get(url=url).json()

        expansions = pd.Series(response)
        return expansions


    def get_event_types(self) -> pd.Series:
        """
        Retrieves all supported event types.

        Returns
        -------
        event_types : pandas.Series
            A list of all supported event types.
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/formats'
        response = self.session.get(url=url).json()

        event_types = pd.Series(response)
        return event_types


    def get_color_ratings(
        self,
        expansion: str,
        start_date: date,
        end_date: date,
        event_type: str = PREMIER_DRAFT,
        combine_splash: bool = False,
        user_group: str | None = None
    ) -> pd.DataFrame:
        """
        Simple statistics per color combination.

        Returns number of games and wins on the multiple color combinations from 17Lands.

        Parameters
        ----------
        expansion : str
            Three-letter code for the expansion set, e.g. 'DMU'.
            Run `get_expansions()` to get a fresh set of the accepted expansions.
        start_date : datetime.date
            Start date for the computed statistics.
        end_date : datetime.date
            End date for the computed statistics.
        event_type : str, default 'PremierDraft'
            The type of event to fetch statistics for, e.g. 'PremierDraft'.
            Run `get_event_types()` for a list of available event types.
            Check the paired `constants` package for the most common values.
        combine_splash : bool, default False
            True if splashing decks data should be aggregated together with
            decks with just main colors.
        user_group : str, optional
            The tier of player data to fetch data from.
            Returns data for all tiers by default.
            Possible values are 'top', 'middle', and 'bottom'.

        Returns
        -------
        color_ratings : pandas.DataFrame
            The number of wins and games played per color combination, one per row.
            The columns are:
                is_summary : bool
                    Whether the row is a summary of multiple color combinations or not.
                color_name : str
                    Long-form color name.
                wins : int
                    Number of wins recorded.
                games : int
                    Number of games recorded.

        Examples
        --------
        >>> get_color_ratings(
                expansion='DMU',
                start_date=datetime.date(2020, 9, 1),
                end_date=datetime.date(2022, 10, 9)
            )
        >>> get_color_ratings(
                expansion='DMU',
                start_date=datetime.date(2020, 9, 1),
                end_date=datetime.date(2022, 10, 9),
                event_type='TradDraft',
                combine_splash=True,
                user_group='top'
            )
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/color_ratings/data'
        params = {
            'expansion': expansion,
            'event_type': event_type,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'combine_splash': combine_splash,
            'user_group': user_group
        }
        response = self.session.get(url=url, params=params).json()

        # Apply a more intuitive columns ordering
        unsorted_df = pd.DataFrame(response)
        columns_order = [
            'is_summary',
            'color_name',
            'wins',
            'games'
        ]
        color_ratings = unsorted_df.loc[:, columns_order]

        return color_ratings


    def get_card_ratings(
        self,
        expansion: str,
        start_date: date,
        end_date: date,
        event_type: str = PREMIER_DRAFT,
        user_group: str | None = None,
        deck_colors: str | None = None
    ) -> pd.DataFrame:
        """
        Card statistics.

        Returns a pletora of statistics on individual cards.

        Parameters
        ----------
        expansion : str
            Three-letter code for the expansion set, e.g. 'DMU'.
            Run `get_expansions()` to get a fresh set of the accepted expansions.
        start_date : datetime.date
            Start date for the computed statistics.
        end_date : datetime.date
            End date for the computed statistics.
        event_type : str, default 'PremierDraft'
            The type of event to fetch statistics for, e.g. 'PremierDraft'.
            Run `get_event_types()` for a list of available event types.
            Check the paired `constants` package for the most common values.
        user_group : str, optional
            The tier of player data to fetch data from.
            Returns data for all tiers by default.
            Possible values are 'top', 'middle', and 'bottom'.
        deck_colors : str, optional
            The decks' color combination to obtain statistics from.
            Defaults to aggregated data across all color combinations.

        Returns
        -------
        card_ratings : pandas.DataFrame
            Statistics per card, one per row.
            The columns are (extended metrics definitions
            at https://www.17lands.com/metrics_definitions):
                name : str
                    Name of the card.
                color : str
                    Color of the card.
                rarity : str
                    Rarity of the card.
                seen_count : int
                    The number of packs in which a card was seen.
                avg_last_seen_at : float
                    The average pick number where the card was last seen in packs.
                pick_count : int
                    The number of instances of the card picked by 17Lands drafters.
                avg_taken_at : float
                    The average pick number at which the card was taken by 17Lands drafters.
                games_played_count : int
                    The number of games played with the card in the maindeck,
                    multiplied by the number of copies.
                games_played_win_rate : float
                    The win rate of decks with at least one copy of the card in the maindeck,
                    weighted by the number of copies in the deck.
                opening_hand_game_count : int
                    The number of games where an instance of the card was in the opening hand.
                opening_hand_win_rate : float
                    The win rate of games where an instance of the card was in the opening hand.
                drawn_game_count : int
                    The number of times the card was drawn from the deck into hand,
                    not counting the opening hand.
                drawn_win_rate : float
                    The win rate of games where an instance of the card was drawn from the deck
                    into hand, not counting cards from the opening hand.
                in_hand_game_count : int
                    The number of times the card was drawn into hand, either in the opening hand
                    or later.
                in_hand_win_rate : float
                    The win rate of games where an instance of the card was drawn into hand,
                    either in the opening hand or later.
                not_drawn_game_count : int
                    The copies of a card that were in the maindeck, minus the number of copies
                    that were drawn.
                not_drawn_win_rate : float
                    The win rate in games where one or more copies of the card
                    was in the maindeckbut not drawn at any stage of the game,
                    weighted by the number of copies not drawn.
                improvement_when_drawn : float
                    The difference between in_hand_win_rate and not_drawn_win_rate.

        Examples
        --------
        >>> get_card_ratings(
                expansion='DMU',
                start_date=datetime.date(2020, 9, 1),
                end_date=datetime.date(2022, 10, 9)
            )
        >>> get_card_ratings(
                expansion='DMU',
                start_date=datetime.date(2020, 9, 1),
                end_date=datetime.date(2022, 10, 9),
                event_type='TradDraft',
                user_group='top',
                deck_colors='UR'
            )
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/card_ratings/data'
        params = {
            'expansion': expansion,
            'format': event_type,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'user_group': user_group,
            'colors': deck_colors
        }
        response = self.session.get(url=url, params=params).json()

        # Apply a more intuitive columns ordering, and remove URLs and sideboard metrics
        unsorted_df = pd.DataFrame(response)
        sorted_cols = [
            'name',
            'color',
            'rarity',
            'seen_count',
            'avg_seen',
            'pick_count',
            'avg_pick',
            'game_count',
            'win_rate',
            'opening_hand_game_count',
            'opening_hand_win_rate',
            'drawn_game_count',
            'drawn_win_rate',
            'ever_drawn_game_count',
            'ever_drawn_win_rate',
            'never_drawn_game_count',
            'never_drawn_win_rate',
            'drawn_improvement_win_rate'
        ]
        sorted_df = unsorted_df.loc[:, sorted_cols]

        # Rename the metrics for more standard ones
        card_ratings = sorted_df.rename(columns={
            'avg_seen': 'avg_last_seen_at',
            'avg_pick': 'avg_taken_at',
            'game_count': 'games_played_count',
            'win_rate': 'games_played_win_rate',
            'ever_drawn_game_count': 'in_hand_game_count',
            'ever_drawn_win_rate': 'in_hand_win_rate',
            'never_drawn_game_count': 'not_drawn_game_count',
            'never_drawn_win_rate': 'not_drawn_win_rate',
            'drawn_improvement_win_rate': 'improvement_when_drawn'
        })

        return card_ratings


    def get_card_evaluations(
        self,
        expansion: str,
        start_date: date,
        end_date: date,
        event_type: str = PREMIER_DRAFT,
        rarity: str | None = None,
        color: str | None = None
    ) -> pd.DataFrame:
        """
        Card evaluations through time.

        Returns metrics on card evaluation through time.

        Parameters
        ----------
        expansion : str
            Three-letter code for the expansion set, e.g. 'DMU'.
            Run `get_expansions()` to get a fresh set of the accepted expansions.
        start_date : datetime.date
            Start date for the computed statistics.
        end_date : datetime.date
            End date for the computed statistics.
        event_type : str, default 'PremierDraft'
            The type of event to fetch statistics for, e.g. 'PremierDraft'.
            Run `get_event_types()` for a list of available event types.
            Check the paired `constants` package for the most common values.
        rarity : str
            The rarity of the cards to obtain statistics from.
            Possible values are 'common', 'uncommon', 'rare', or 'mythic'
            Check the paired `constants` package for helper constants.
        color : str, optional
            The cards' color to obtain statistics from.
            Can be either simple colors ('W', 'U', 'B', 'R', or 'G'), 'Colorless', or 'Multicolor'.
            Check the paired `constants` package for helper constants.
            Defaults to all colors.

        Returns
        -------
        card_evaluations : pandas.DataFrame
            One row per date-card combination of valuation statistics.
            The columns are:
                date : datetime.date
                    The date for which the statistics were computed.
                name : str
                    The card name.
                pick_count : int
                    The number of instances of the card picked by 17Lands drafters.
                avg_taken_at : float
                    The average pick number at which the card was taken by 17Lands drafters.
                seen_count : int
                    The number of packs in which a card was seen.
                avg_last_seen_at : float
                    The average pick number where the card was last seen in packs.

        Examples
        --------
        >>> get_card_evaluations(
                expansion='DMU',
                start_date=datetime.date(2020, 9, 1),
                end_date=datetime.date(2022, 10, 9)
            )
        >>> get_card_evaluations(
                expansion='DMU',
                start_date=datetime.date(2020, 9, 1),
                end_date=datetime.date(2022, 10, 9),
                event_type='TradDraft',
                rarity='uncommon',
                color='Multicolor'
            )
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/card_evaluation_metagame/data'
        params = {
            'expansion': expansion,
            'format': event_type,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'rarity': rarity,
            'color': color
        }
        response = self.session.get(url=url, params=params).json()

        # Tidy up data into a dataframe of one row per date-card combination
        digested_response_accum = []
        for d_i, day in enumerate(response['dates']):
            for c_i, card in enumerate(response['cards']):
                digested_response_accum.append({
                    'date': datetime.strptime(day, '%Y-%m-%d'),
                    'name': card,
                    'pick_n': response['data'][d_i][c_i]['pick_n'],
                    'pick_avg': response['data'][d_i][c_i]['pick_avg'],
                    'seen_n': response['data'][d_i][c_i]['seen_n'],
                    'seen_avg': response['data'][d_i][c_i]['seen_avg']
                })
        digested_response_df = pd.DataFrame(digested_response_accum).drop_duplicates(ignore_index=True)

        # Rename the metrics for more standard ones
        card_evaluations = digested_response_df.rename(columns={
            'pick_n': 'pick_count',
            'pick_avg': 'avg_taken_at',
            'seen_n': 'seen_count',
            'seen_avg': 'avg_last_seen_at'
        })

        return card_evaluations


    def get_play_draw_stats(self) -> pd.DataFrame:
        """
        Play/draw statistics.

        Returns the play/draw statiscs per set.

        Returns
        -------
        play_draw_stats : pandas.DataFrame
            One row per format-event type combination of statistics.
            The columns are:
                expansion : str
                    The expansion the statistics correspond to.
                event_type : str
                    The event type the statistics correspond to.
                avg_game_length : float
                    The average number of turns each game took.
                on_play_win_rate : float
                    The win rate when on the play.
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/play_draw'
        response = self.session.get(url=url).json()

        play_draw_stats = pd.DataFrame(response)
        return play_draw_stats


    def get_trophy_decks(
        self,
        expansion: str,
        event_type: str = PREMIER_DRAFT
    ) -> pd.DataFrame:
        """
        Recent trophy decks.

        Returns details on recent decks.
        Use `draft_id` and `deck_index` to get the draft information via `get_draft()`
        and deck via `get_deck()`.

        Parameters
        ----------
        expansion : str
            Three-letter code for the expansion set, e.g. 'DMU'.
            Run `get_expansions()` to get a fresh set of the accepted expansions.
        event_type : str, default 'PremierDraft'
            The type of event to fetch statistics for, e.g. 'PremierDraft'.
            Run `get_event_types()` for a list of available event types.
            Check the paired `constants` package for the most common values.

        Returns
        -------
        trophy_decks : pandas.DataFrame
            One row per date-card combination of valuation statistics.
            The columns are:
                time : datetime.datetime
                    The time the deck was created.
                colors : str
                    The color combination of the deck.
                wins : int
                    The number of wins the deck achieved.
                losses : int
                    The number of losses the deck suffered.
                start_rank : str
                    The player's rank when the deck started being used.
                end_rank : str
                    The player's rank when the deck stopped being used.
                draft_id : str
                    The identifier of the draft that originated the draft.
                deck_index : int
                    Identifies the deck given the draft.

        Examples
        --------
        >>> get_trophy_decks('DMU')
        >>> get_trophy_decks(
                expansion='DMU',
                event_type='TradDraft
            )
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/trophies'
        params = {
            'expansion': expansion,
            'format': event_type
        }
        response = self.session.get(url=url, params=params).json()

        # Apply a more intuitive columns ordering
        unsorted_df = pd.DataFrame(response)
        sorted_cols = [
            'time',
            'colors',
            'wins',
            'losses',
            'start_rank',
            'end_rank',
            'aggregate_id',
            'deck_index',
        ]
        sorted_df = unsorted_df.loc[:, sorted_cols]

        # Keep column names uniform and intuitive
        trophy_decks = sorted_df.rename(columns={'aggregate_id': 'draft_id'})

        # Change time column data type
        trophy_decks.loc[:, 'time'] = pd.to_datetime(trophy_decks.time)

        return trophy_decks


    def get_draft(self, draft_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Draft information.

        Returns pick-by-pickthe details on a given draft.

        Parameters
        ----------
        draft_id : str
            The unique identifier of the draft of interest.

        Returns
        -------
        picks : pandas.DataFrame
            Pick-by-pick details on cards available, picked, and so on.
            The columns are:
                expansion : str
                    The draft's expansion.
                pack_number : int
                    The pack number. First pack is 0.
                pick_number : int
                    The pick number. First pick is 0.
                colors : str
                    The color combination of the deck, as per 17Lands algorithm.
                pick : str
                    The card being picked.
                available : list[str]
                    The names for the available cards in the pack.
                known_missing : list[str]
                    The names for the cards known missing from the pack.
                pool : list[str]
                    The name of the cards picked by the player.
                possible_maindeck : list[str]
                    The names of the cards selected by the player as maindeck.
                probable_sideboard : list[str]
                    The names of the cards selected by the player as sideboard.
        cards_performance : pandas.DataFrame
            Card-level statistics computed at the time of the draft.
            The columns are:
                name : str
                    Name of the card.
                seen_count : int
                    The number of packs in which a card was seen.
                avg_last_seen_at : float
                    The average pick number where the card was last seen in packs.
                pick_count : int
                    The number of instances of the card picked by 17Lands drafters.
                avg_taken_at : float
                    The average pick number at which the card was taken by 17Lands drafters.

        Examples
        --------
        >>> get_draft('d4ce12d252824d699372e7d2ec82f813')
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/draft/stream'
        params = {
            'draft_id': draft_id
        }
        response = self.session.get(url=url, params=params)

        # Process built-in JSON
        response_obj = json.loads(response.text[6:-2])

        # Only return content if payload is complete
        if response_obj['type'] != 'complete':
            raise ValueError(f"Response is not complete. Response type: '{response_obj['type']}'")

        # Parse payload
        payload = response_obj['payload']
        expansion = payload['expansion']

        # Parse picks
        picks_accum = []
        for pick in payload['picks']:
            picks_accum.append({
                'expansion': expansion,
                'pack_number': pick['pack_number'],
                'pick_number': pick['pick_number'],
                'colors': pick['colors'],
                'pick': pick['pick']['name'],
                'available': [a['name'] for a in pick['available']],
                'known_missing': [m['name'] for m in pick['known_missing']],
                'pool': [p['name'] for p in pick['pool']],
                'possible_maindeck': [
                    m['name']
                    for m in [
                        i for l in pick['possible_maindeck']
                        for i in l
                    ]
                ],
                'probable_sideboard': [
                    s['name']
                    for s in [
                        i for l in pick['probable_sideboard']
                        for i in l
                    ]
                ]
            })
        picks = pd.DataFrame(picks_accum)

        # Parse and transform card performancef data
        cards_performance = pd.DataFrame(payload['card_performance_data']) \
            .transpose() \
            .reset_index() \
            .rename(columns={
                'index': 'name',
                'total_times_seen': 'seen_count',
                'avg_seen_position': 'avg_last_seen_at',
                'total_times_picked': 'pick_count',
                'avg_pick_position': 'avg_taken_at'
            })

        return picks, cards_performance


    def get_deck(self, draft_id: str, deck_index: int) -> Tuple[pd.DataFrame, dict[str, str]]:
        """
        Deck information.

        Returns a deck's contents, along with some metadata.

        Parameters
        ----------
        draft_id : str
            The unique identifier of the draft of interest.
        deck_index : int
            The index of the deck, given the draft.

        Returns
        -------
        deck : pandas.DataFrame
            The cards that constitute the deck, both maindeck and sideboard.
            The columns are:
                group : str
                    The name of the deck's group the card belongs to.
                    Can be either `Maindeck` or `Sideboard`.
                name : str
                    The name of the card.
        deck_metadata : dict[str, str]
            Some accompanying metadata for the deck.
            The fields are:
                expansion : str
                    Three-letter code for the expansion set, e.g. 'DMU'.
                    Run `get_expansions()` to get a fresh set of the accepted expansions.
                event_type : str
                    The type of event to fetch statistics for, e.g. 'PremierDraft'.
                    Run `get_event_types()` for a list of available event types.
                wins : int
                    The number of wins the deck achieved.
                losses : int
                    Number of losses the deck suffered.
                pool_link : str
                    The relative path for the pool within 17Lands website.
                deck_links : list[str]
                    The relative path for the deck within 17Lands website.
                details_link : str
                    The relative path for the draft details within 17Lands website.
                draft_link : str
                    The relative path for the draft's picks within 17Lands website.
                sealed_deck_tech_link : str
                    The link for SealedDeck.Tech pre-filled with the deck's pool.
        """
        # Prepare and perform API request
        url = 'https://www.17lands.com/data/deck'
        params = {
            'draft_id': draft_id,
            'deck_index': deck_index
        }
        response = self.session.get(url=url, params=params).json()

        # Compile a deck dataframe
        deck_accum = []
        for group in response['groups']:
            for card in group['cards']:
                deck_accum.append({
                    'group': group['name'],
                    'name': card['name']
                })
        deck = pd.DataFrame(deck_accum)

        # Compile deck metadata
        event_info = response['event_info']
        deck_metadata = Box({
            'expansion': event_info['expansion'],
            'event_type': event_info['format'],
            'wins': event_info['wins'],
            'losses': event_info['losses'],
            'pool_link': event_info['pool_link'],
            'deck_links': event_info['deck_links'],
            'details_link': event_info['details_link'],
            'draft_link': event_info['draft_link'],
            'sealed_deck_tech_link': response['builder_link']
        })

        return deck, deck_metadata
